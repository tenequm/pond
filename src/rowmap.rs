//! Memory-mapped, process-shareable `row_id -> (session_id, message_id)` map.
//!
//! FTS/vector retrievers return Lance `_rowid`s; turning those into the
//! `(session_id, message_id)` a response needs would otherwise be a scattered
//! data-file take (~54 GETs/query on a remote store - the dominant FTS arm
//! cost). This map resolves them from local storage instead.
//!
//! The map is a single immutable file (`build` writes it via temp + atomic
//! rename) that callers `mmap`. Because it is `mmap`'d, the OS page cache holds
//! one physical copy of the hot pages regardless of how many pond processes on
//! the box map the same file - so N instances share the RAM, not multiply it,
//! and a restart re-`open`s instantly instead of rescanning the store.
//!
//! Layout: `Header | [Record; count] (sorted by row_id) | blob`. Lookup is a
//! binary search over the records (page-cache-resident, ~21 compares for 2M
//! rows) plus two slices into the blob. pond datasets use stable row ids
//! (`enable_stable_row_ids`), so a built map stays valid across compaction and
//! only needs rebuilding when the dataset version advances.

use std::fs::File;
use std::io::Write;
use std::mem::size_of;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use bytemuck::{Pod, Zeroable};
use memmap2::Mmap;

const MAGIC: [u8; 8] = *b"PONDRKM1";

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Header {
    magic: [u8; 8],
    /// Messages-table dataset version this map was built from.
    version: u64,
    count: u64,
    /// Byte offset where the blob region starts.
    blob_offset: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Record {
    row_id: u64,
    /// Offset of this row's bytes within the blob region.
    blob_off: u64,
    sid_len: u32,
    mid_len: u32,
}

/// An open, memory-mapped row-key map. Cheap to clone the path; `lookup` is
/// lock-free and reentrant.
pub struct RowKeyMap {
    mmap: Mmap,
    version: u64,
    count: usize,
    blob_offset: usize,
}

impl std::fmt::Debug for RowKeyMap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowKeyMap")
            .field("version", &self.version)
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
}

impl RowKeyMap {
    /// Deterministic file path for a store's map at a given version, under
    /// `cache_dir`. Same store + version -> same path, so sibling pond
    /// processes mmap one shared file.
    pub fn path_for(cache_dir: &Path, store_key: &str, version: u64) -> PathBuf {
        cache_dir.join(format!("rowkeymap-{store_key}-v{version}.rkm"))
    }

    /// Write the map file from `entries` (any order) via temp + atomic rename.
    /// Holds the entries plus an equal-size blob in memory for the duration;
    /// this is the one-time build, not the steady-state footprint.
    pub fn build(path: &Path, version: u64, mut entries: Vec<(u64, String, String)>) -> Result<()> {
        entries.sort_unstable_by_key(|(row_id, _, _)| *row_id);

        let mut records = Vec::with_capacity(entries.len());
        let mut blob: Vec<u8> = Vec::new();
        for (row_id, sid, mid) in &entries {
            let blob_off = blob.len() as u64;
            blob.extend_from_slice(sid.as_bytes());
            blob.extend_from_slice(mid.as_bytes());
            records.push(Record {
                row_id: *row_id,
                blob_off,
                sid_len: u32::try_from(sid.len()).context("session_id too long")?,
                mid_len: u32::try_from(mid.len()).context("message_id too long")?,
            });
        }

        let blob_offset = (size_of::<Header>() + records.len() * size_of::<Record>()) as u64;
        let header = Header {
            magic: MAGIC,
            version,
            count: records.len() as u64,
            blob_offset,
        };

        // Unique temp name per builder (pid + nonce), not a fixed `.tmp`: two
        // pond processes prewarming the same store+version would otherwise share
        // one temp inode - the second's `O_TRUNC` create would mutate the file
        // the first is mapping, breaking `open`'s immutability contract. With a
        // unique name, the atomic rename is the only publish.
        let tmp = path.with_extension(format!(
            "tmp-{}-{:016x}",
            std::process::id(),
            fastrand::u64(..)
        ));
        {
            let mut file = File::create(&tmp)
                .with_context(|| format!("create row-key map temp {}", tmp.display()))?;
            file.write_all(bytemuck::bytes_of(&header))?;
            file.write_all(bytemuck::cast_slice(&records))?;
            file.write_all(&blob)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename row-key map into place {}", path.display()))?;
        Ok(())
    }

    /// `mmap` an existing map file. The mapping is lazy: pages fault in (and
    /// join the shared page cache) on first touch by `lookup`.
    pub fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("open row-key map {}", path.display()))?;
        // SAFETY: the file is immutable once renamed into place (build writes a
        // temp then atomic-renames), so the mapping never sees concurrent
        // truncation/mutation.
        #[allow(unsafe_code)]
        let mmap = unsafe { Mmap::map(&file)? };
        ensure!(
            mmap.len() >= size_of::<Header>(),
            "row-key map {} too small for header",
            path.display()
        );
        let header: Header = *bytemuck::from_bytes(&mmap[..size_of::<Header>()]);
        ensure!(
            header.magic == MAGIC,
            "row-key map {} bad magic",
            path.display()
        );
        let count = usize::try_from(header.count).context("count overflow")?;
        let blob_offset = usize::try_from(header.blob_offset).context("blob_offset overflow")?;
        let records_end = size_of::<Header>() + count * size_of::<Record>();
        ensure!(
            blob_offset == records_end && mmap.len() >= blob_offset,
            "row-key map {} layout mismatch",
            path.display()
        );
        Ok(Self {
            mmap,
            version: header.version,
            count,
            blob_offset,
        })
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn records(&self) -> &[Record] {
        let start = size_of::<Header>();
        let end = start + self.count * size_of::<Record>();
        bytemuck::cast_slice(&self.mmap[start..end])
    }

    /// Resolve a `row_id` to `(session_id, message_id)`. `None` if the id is not
    /// in this map (e.g. a row appended after the map was built - the caller
    /// falls back to a data take for those).
    pub fn lookup(&self, row_id: u64) -> Option<(&str, &str)> {
        let records = self.records();
        let idx = records
            .binary_search_by_key(&row_id, |record| record.row_id)
            .ok()?;
        let record = &records[idx];
        let base = self.blob_offset + record.blob_off as usize;
        let mid_start = base + record.sid_len as usize;
        let mid_end = mid_start + record.mid_len as usize;
        // Checked slices keep `lookup` total: a truncated/corrupt map whose
        // record extents run past the blob yields `None` (-> the caller's take
        // fallback) instead of panicking.
        let sid = std::str::from_utf8(self.mmap.get(base..mid_start)?).ok()?;
        let mid = std::str::from_utf8(self.mmap.get(mid_start..mid_end)?).ok()?;
        Some((sid, mid))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn build_open_lookup_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = RowKeyMap::path_for(dir.path(), "teststore", 7);
        let entries = vec![
            (10u64, "sess-a".to_owned(), "msg-1".to_owned()),
            (3u64, "sess-b/agent-x".to_owned(), "msg-2".to_owned()),
            (99u64, "sess-c".to_owned(), "msg-3".to_owned()),
        ];
        RowKeyMap::build(&path, 7, entries).unwrap();

        let map = RowKeyMap::open(&path).unwrap();
        assert_eq!(map.version(), 7);
        assert_eq!(map.len(), 3);
        assert_eq!(map.lookup(10), Some(("sess-a", "msg-1")));
        assert_eq!(map.lookup(3), Some(("sess-b/agent-x", "msg-2")));
        assert_eq!(map.lookup(99), Some(("sess-c", "msg-3")));
        assert_eq!(map.lookup(42), None);
    }

    #[test]
    fn empty_map_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let path = RowKeyMap::path_for(dir.path(), "empty", 1);
        RowKeyMap::build(&path, 1, Vec::new()).unwrap();
        let map = RowKeyMap::open(&path).unwrap();
        assert!(map.is_empty());
        assert_eq!(map.lookup(0), None);
    }
}
