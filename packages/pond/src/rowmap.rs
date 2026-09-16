//! Memory-mapped, process-shareable per-message meta map keyed by stable
//! `row_id`. Resolves FTS/vector `_rowid`s to `(session_id, message_id)` and
//! hydrates hit meta (`role`, `project`, `source_agent`, `timestamp`,
//! `search_text`) in memory, with a `take_rows` miss-fallback for rows appended
//! since the build. Also carries a `session_id -> message count` aggregate.
//!
//! `session_id`/`project`/`source_agent`/`role` are dictionary-encoded (each
//! distinct value stored once, referenced by `u32` index); `search_text` is
//! block-compressed ([`BLOCK_ROWS`] rows per zstd block). On the real 2M-message
//! corpus that takes the map from ~655 MB (flat) to ~270 MB.
//!
//! Encoded row by row by [`RowMetaBuilder`], which holds one block of text and
//! the dictionaries rather than the corpus, then published via temp + atomic
//! rename and `mmap`'d read-only, so N pond processes on the box share one
//! physical copy in the OS page cache and a restart re-`open`s instantly.
//! Stable row ids (`enable_stable_row_ids`) keep a built map valid across
//! compaction; it only rebuilds when the dataset version advances.
//!
//! Layout: `Header | [Record; count] | [SessionEntry] | [DictEntry; project] |
//! [DictEntry; agent] | [DictEntry; role] | [BlockEntry] | blob`. Records are
//! sorted by `row_id` (binary search); a row's block is `record_index /
//! BLOCK_ROWS`. The blob holds the compressed `search_text` blocks, then per-row
//! `{36-byte header + message_id}`, then per-session `session_id` bytes, then the
//! dict value bytes. Each session entry also carries its max message timestamp,
//! the watermark the `pond sync` skip oracle compares against the source.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, ensure};
use bytemuck::{Pod, Zeroable};
use memmap2::Mmap;

const MAGIC: [u8; 8] = *b"PONDRMM5";
const BLOCK_ROWS: usize = 256;
const ZSTD_LEVEL: i32 = 3;

/// Per-row blob header: `timestamp_micros` (i64 LE) then seven `u32` LE fields -
/// the four dictionary indices (`session`, `project`, `source_agent`, `role`),
/// the `message_id` length, and the `search_text` offset+length within its
/// decompressed block.
const ROW_HEADER_LEN: usize = 8 + 7 * 4;

/// One-slot decompressed-block cache `(block_index, plaintext)`, threaded
/// through a batch of `lookup_meta` calls so rows sharing a block (a session's
/// hits are row-id-adjacent) decompress it once, not once per row.
type BlockCache = Option<(usize, Vec<u8>)>;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Header {
    magic: [u8; 8],
    version: u64,
    count: u64,
    session_count: u64,
    project_count: u64,
    agent_count: u64,
    role_count: u64,
    block_count: u64,
    blob_offset: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Record {
    row_id: u64,
    blob_off: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct SessionEntry {
    sid_off: u64,
    max_ts_micros: i64,
    sid_len: u32,
    count: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct DictEntry {
    off: u64,
    len: u32,
    _pad: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct BlockEntry {
    comp_off: u64,
    comp_len: u32,
    decomp_len: u32,
}

/// Owned input row for [`RowMetaMap::build`].
#[derive(Clone)]
pub struct RowMetaEntry {
    pub row_id: u64,
    pub session_id: String,
    pub message_id: String,
    pub role: String,
    pub project: String,
    pub source_agent: String,
    pub timestamp_micros: i64,
    pub search_text: String,
}

impl RowMetaEntry {
    /// Borrow this row for [`RowMetaBuilder::push`].
    fn as_row(&self) -> RowMetaRef<'_> {
        RowMetaRef {
            row_id: self.row_id,
            session_id: &self.session_id,
            message_id: &self.message_id,
            role: &self.role,
            project: &self.project,
            source_agent: &self.source_agent,
            timestamp_micros: self.timestamp_micros,
            search_text: &self.search_text,
        }
    }
}

/// Borrowed input row for [`RowMetaBuilder::push`] - [`RowMetaEntry`]'s fields
/// without the six owned strings, so a scan batch or an mmap'd segment feeds the
/// builder without allocating a row at a time.
pub struct RowMetaRef<'a> {
    pub row_id: u64,
    pub session_id: &'a str,
    pub message_id: &'a str,
    pub role: &'a str,
    pub project: &'a str,
    pub source_agent: &'a str,
    pub timestamp_micros: i64,
    pub search_text: &'a str,
}

impl RowMetaRef<'_> {
    /// Copy every field out of whatever this row borrows - the input the
    /// sorting rebuild needs, which outlives the cursor that produced it.
    fn to_entry(&self) -> RowMetaEntry {
        RowMetaEntry {
            row_id: self.row_id,
            session_id: self.session_id.to_owned(),
            message_id: self.message_id.to_owned(),
            role: self.role.to_owned(),
            project: self.project.to_owned(),
            source_agent: self.source_agent.to_owned(),
            timestamp_micros: self.timestamp_micros,
            search_text: self.search_text.to_owned(),
        }
    }
}

/// Rows reached [`RowMetaBuilder::push`] out of `row_id` order. The builder
/// encodes rows in arrival order (records are binary-searched, so that order is
/// the file's), and it has already dropped the text of every block it closed -
/// so it cannot reorder after the fact. Callers streaming from a source whose
/// order is not guaranteed catch this and fall back to the buffering
/// [`RowMetaMap::build`], which sorts first.
#[derive(Debug, thiserror::Error)]
#[error("row meta rows arrived out of row_id order ({previous} then {current})")]
pub struct UnorderedRows {
    pub previous: u64,
    pub current: u64,
}

/// Cold base builds that could not stream and re-encoded through the buffering
/// [`RowMetaMap::build`] instead. Process-lifetime, monotonic, `Relaxed` - a
/// counter, not a synchronization point. Private so the only way to move it is
/// [`note_rowmap_scan_fallback`].
static ROWMAP_SCAN_FALLBACKS: AtomicU64 = AtomicU64::new(0);

/// Record one cold build that fell back off the streaming path.
pub(crate) fn note_rowmap_scan_fallback() {
    ROWMAP_SCAN_FALLBACKS.fetch_add(1, Ordering::Relaxed);
}

/// Cold-build fallbacks so far this process.
///
/// The streaming encoder needs ascending `row_id`s, and a Lance scan only
/// delivers them when a fragment order exists that produces them (see
/// `Store::build_rowmap_from_scan`). Live row ids can interleave across
/// fragments, and a compaction can bake a non-ascending order into a single
/// fragment, so no plan always exists. When none does the build costs what it
/// cost before this encoder - a silent performance cliff that only a
/// `tracing::warn!` marked, invisible to CI and to the memory gate. Counting it
/// lets the bench lane read the number across a scenario and fail on a
/// regression into the slow path.
pub fn rowmap_scan_fallbacks() -> u64 {
    ROWMAP_SCAN_FALLBACKS.load(Ordering::Relaxed)
}

/// Borrowed view of one row's meta. The dictionary-encoded fields borrow the
/// mmap; `search_text` is owned (decompressed from its block).
pub struct RowMeta<'a> {
    pub session_id: &'a str,
    pub message_id: &'a str,
    pub role: &'a str,
    pub project: &'a str,
    pub source_agent: &'a str,
    pub timestamp_micros: i64,
    pub search_text: String,
}

/// An open, memory-mapped row meta map. `lookup`, `lookup_meta`, and
/// `lookup_count` are lock-free and reentrant.
pub struct RowMetaMap {
    mmap: Mmap,
    version: u64,
    count: usize,
    session_count: usize,
    project_count: usize,
    agent_count: usize,
    role_count: usize,
    block_count: usize,
    sessions_off: usize,
    projects_off: usize,
    agents_off: usize,
    roles_off: usize,
    blocks_off: usize,
    blob_offset: usize,
}

impl std::fmt::Debug for RowMetaMap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowMetaMap")
            .field("version", &self.version)
            .field("count", &self.count)
            .field("session_count", &self.session_count)
            .finish_non_exhaustive()
    }
}

impl RowMetaMap {
    /// Base segment path (`-v{version}`): the foot of the LSM chain.
    pub fn path_for(cache_dir: &Path, store_key: &str, version: u64) -> PathBuf {
        cache_dir.join(format!("rowmetamap-{store_key}-v{version}.rmm"))
    }

    /// Delta segment path (`-d{version}`): rows appended since the previous
    /// segment, layered over the base.
    pub fn delta_path(cache_dir: &Path, store_key: &str, version: u64) -> PathBuf {
        cache_dir.join(format!("rowmetamap-{store_key}-d{version}.rmm"))
    }

    /// Encode `entries` into a segment at `path`. Buffering entry point: the
    /// whole corpus is already owned here, so it sorts and replays into
    /// [`RowMetaBuilder`]. A caller that can stream rows in `row_id` order
    /// should drive the builder directly and never materialize this `Vec`.
    pub fn build(path: &Path, version: u64, mut entries: Vec<RowMetaEntry>) -> Result<()> {
        entries.sort_unstable_by_key(|entry| entry.row_id);
        let mut builder = RowMetaBuilder::new(path, version, entries.len())?;
        for entry in &entries {
            builder.push(entry.as_row())?;
        }
        builder.finish()
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("open row meta map {}", path.display()))?;
        // SAFETY: the file is immutable once renamed into place, so the mapping
        // never sees concurrent truncation/mutation.
        #[allow(unsafe_code)]
        let mmap = unsafe { Mmap::map(&file)? };
        ensure!(
            mmap.len() >= size_of::<Header>(),
            "row meta map {} too small for header",
            path.display()
        );
        let header: Header = *bytemuck::from_bytes(&mmap[..size_of::<Header>()]);
        ensure!(
            header.magic == MAGIC,
            "row meta map {} bad magic",
            path.display()
        );
        let count = usize::try_from(header.count).context("count overflow")?;
        let session_count = usize::try_from(header.session_count).context("session_count")?;
        let project_count = usize::try_from(header.project_count).context("project_count")?;
        let agent_count = usize::try_from(header.agent_count).context("agent_count")?;
        let role_count = usize::try_from(header.role_count).context("role_count")?;
        let block_count = usize::try_from(header.block_count).context("block_count")?;
        let blob_offset = usize::try_from(header.blob_offset).context("blob_offset overflow")?;

        let sessions_off = size_of::<Header>() + count * size_of::<Record>();
        let projects_off = sessions_off + session_count * size_of::<SessionEntry>();
        let agents_off = projects_off + project_count * size_of::<DictEntry>();
        let roles_off = agents_off + agent_count * size_of::<DictEntry>();
        let blocks_off = roles_off + role_count * size_of::<DictEntry>();
        let blob_offset_expected = blocks_off + block_count * size_of::<BlockEntry>();
        ensure!(
            blob_offset == blob_offset_expected && mmap.len() >= blob_offset,
            "row meta map {} layout mismatch",
            path.display()
        );
        Ok(Self {
            mmap,
            version: header.version,
            count,
            session_count,
            project_count,
            agent_count,
            role_count,
            block_count,
            sessions_off,
            projects_off,
            agents_off,
            roles_off,
            blocks_off,
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

    /// Highest `row_id` in this segment (records are row_id-sorted), or `None`
    /// when empty. With stable row ids this is the append high-water mark.
    pub fn max_row_id(&self) -> Option<u64> {
        self.records().last().map(|record| record.row_id)
    }

    fn records(&self) -> &[Record] {
        let start = size_of::<Header>();
        let end = start + self.count * size_of::<Record>();
        bytemuck::cast_slice(&self.mmap[start..end])
    }

    fn session_entries(&self) -> &[SessionEntry] {
        let end = self.sessions_off + self.session_count * size_of::<SessionEntry>();
        bytemuck::cast_slice(&self.mmap[self.sessions_off..end])
    }

    fn dict_entries(&self, start: usize, count: usize) -> &[DictEntry] {
        let end = start + count * size_of::<DictEntry>();
        bytemuck::cast_slice(&self.mmap[start..end])
    }

    fn block_entries(&self) -> &[BlockEntry] {
        let end = self.blocks_off + self.block_count * size_of::<BlockEntry>();
        bytemuck::cast_slice(&self.mmap[self.blocks_off..end])
    }

    /// `""` on a corrupt/truncated extent - treated as a miss, never a panic.
    fn blob_str(&self, off: u64, len: u32) -> &str {
        let base = self.blob_offset.saturating_add(off as usize);
        let end = base.saturating_add(len as usize);
        self.mmap
            .get(base..end)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .unwrap_or_default()
    }

    fn session_str(&self, index: usize) -> &str {
        match self.session_entries().get(index) {
            Some(entry) => self.blob_str(entry.sid_off, entry.sid_len),
            None => "",
        }
    }

    fn dict_str(&self, start: usize, count: usize, index: usize) -> &str {
        match self.dict_entries(start, count).get(index) {
            Some(entry) => self.blob_str(entry.off, entry.len),
            None => "",
        }
    }

    /// `(record index, blob header offset)` for `row_id`, `None` if unmapped.
    fn locate(&self, row_id: u64) -> Option<(usize, usize)> {
        let records = self.records();
        let idx = records
            .binary_search_by_key(&row_id, |record| record.row_id)
            .ok()?;
        let base = self
            .blob_offset
            .checked_add(usize::try_from(records[idx].blob_off).ok()?)?;
        Some((idx, base))
    }

    /// Resolve a `row_id` to `(session_id, message_id)` - the cheap arm-
    /// resolution path. Never decompresses a block. `None` for rows appended
    /// after the build (caller falls back to a data take).
    pub fn lookup(&self, row_id: u64) -> Option<(&str, &str)> {
        let (_, base) = self.locate(row_id)?;
        let header = self.mmap.get(base..base.checked_add(ROW_HEADER_LEN)?)?;
        let session_idx = read_u32(header, 8)?;
        let mid_len = read_u32(header, 24)?;
        let mut at = base + ROW_HEADER_LEN;
        let mid = self.slice_str(&mut at, mid_len)?;
        Some((self.session_str(session_idx), mid))
    }

    /// Resolve a `row_id` to its full hydration meta, decompressing the row's
    /// `search_text` block (reusing `cache` if it already holds that block).
    /// `None` if unmapped (caller falls back to take_rows).
    pub fn lookup_meta(&self, row_id: u64, cache: &mut BlockCache) -> Option<RowMeta<'_>> {
        let (idx, base) = self.locate(row_id)?;
        let header = self.mmap.get(base..base.checked_add(ROW_HEADER_LEN)?)?;
        let timestamp_micros = i64::from_le_bytes(header.get(0..8)?.try_into().ok()?);
        let session_idx = read_u32(header, 8)?;
        let project_idx = read_u32(header, 12)?;
        let agent_idx = read_u32(header, 16)?;
        let role_idx = read_u32(header, 20)?;
        let mid_len = read_u32(header, 24)?;
        let text_off = read_u32(header, 28)?;
        let text_len = read_u32(header, 32)?;
        let mut at = base + ROW_HEADER_LEN;
        let message_id = self.slice_str(&mut at, mid_len)?;
        let search_text = self.decompress_text(idx, text_off, text_len, cache)?;
        Some(RowMeta {
            session_id: self.session_str(session_idx),
            message_id,
            role: self.dict_str(self.roles_off, self.role_count, role_idx),
            project: self.dict_str(self.projects_off, self.project_count, project_idx),
            source_agent: self.dict_str(self.agents_off, self.agent_count, agent_idx),
            timestamp_micros,
            search_text,
        })
    }

    fn decompress_block(&self, block_idx: usize) -> Option<Vec<u8>> {
        let block = self.block_entries().get(block_idx)?;
        if block.decomp_len == 0 {
            return Some(Vec::new());
        }
        let comp_base = self.blob_offset.checked_add(block.comp_off as usize)?;
        let comp = self
            .mmap
            .get(comp_base..comp_base.checked_add(block.comp_len as usize)?)?;
        zstd::bulk::decompress(comp, block.decomp_len as usize).ok()
    }

    fn decompress_text(
        &self,
        idx: usize,
        text_off: usize,
        text_len: usize,
        cache: &mut BlockCache,
    ) -> Option<String> {
        if text_len == 0 {
            return Some(String::new());
        }
        let block_idx = idx / BLOCK_ROWS;
        if cache.as_ref().map(|(block, _)| *block) != Some(block_idx) {
            *cache = Some((block_idx, self.decompress_block(block_idx)?));
        }
        let plain = &cache.as_ref()?.1;
        let value = plain.get(text_off..text_off.checked_add(text_len)?)?;
        String::from_utf8(value.to_vec()).ok()
    }

    /// Borrowed view of record `index`, with `search_text` sliced out of
    /// `block_plain` - the decompressed block that record belongs to. Every
    /// other field borrows the mmap, so a whole-segment walk allocates only the
    /// block buffers. `None` on a malformed record, which the walk skips.
    fn row_ref<'a>(&'a self, index: usize, block_plain: &'a [u8]) -> Option<RowMetaRef<'a>> {
        let record = self.records().get(index)?;
        let base = self.blob_offset.checked_add(record.blob_off as usize)?;
        let header = self.mmap.get(base..base.checked_add(ROW_HEADER_LEN)?)?;
        let timestamp_micros = i64::from_le_bytes(header.get(0..8)?.try_into().ok()?);
        let session_idx = read_u32(header, 8)?;
        let project_idx = read_u32(header, 12)?;
        let agent_idx = read_u32(header, 16)?;
        let role_idx = read_u32(header, 20)?;
        let mid_len = read_u32(header, 24)?;
        let text_off = read_u32(header, 28)?;
        let text_len = read_u32(header, 32)?;
        let mut at = base + ROW_HEADER_LEN;
        let message_id = self.slice_str(&mut at, mid_len)?;
        let search_text = if text_len == 0 {
            ""
        } else {
            let bytes = block_plain.get(text_off..text_off.checked_add(text_len)?)?;
            std::str::from_utf8(bytes).ok()?
        };
        Some(RowMetaRef {
            row_id: record.row_id,
            session_id: self.session_str(session_idx),
            message_id,
            role: self.dict_str(self.roles_off, self.role_count, role_idx),
            project: self.dict_str(self.projects_off, self.project_count, project_idx),
            source_agent: self.dict_str(self.agents_off, self.agent_count, agent_idx),
            timestamp_micros,
            search_text,
        })
    }

    /// Whole-session message count for `session_id`. `None` if the session is
    /// not in this map (caller falls back to the `session_id IN (...)` scan).
    pub fn lookup_count(&self, session_id: &str) -> Option<usize> {
        let idx = self.session_index(session_id)?;
        Some(self.session_entries()[idx].count as usize)
    }

    /// Max message timestamp (micros) stored for `session_id` - the watermark the
    /// sync skip oracle compares against the source's latest message timestamp.
    /// `None` if the session is not in this map.
    pub fn lookup_max_ts(&self, session_id: &str) -> Option<i64> {
        let idx = self.session_index(session_id)?;
        Some(self.session_entries()[idx].max_ts_micros)
    }

    fn session_watermarks(&self) -> impl Iterator<Item = (&str, i64)> {
        self.session_entries().iter().map(|entry| {
            (
                self.blob_str(entry.sid_off, entry.sid_len),
                entry.max_ts_micros,
            )
        })
    }

    /// Index into `session_entries` for `session_id` - the shared binary
    /// search behind every per-session accessor.
    fn session_index(&self, session_id: &str) -> Option<usize> {
        self.session_entries()
            .binary_search_by(|entry| self.blob_str(entry.sid_off, entry.sid_len).cmp(session_id))
            .ok()
    }

    /// A record's blob header slice. Checked so a corrupt map yields `None`
    /// (-> caller falls back to the store), not a panic.
    fn header_at(&self, record: &Record) -> Option<&[u8]> {
        let base = self.blob_offset.checked_add(record.blob_off as usize)?;
        self.mmap.get(base..base.checked_add(ROW_HEADER_LEN)?)
    }

    /// Resolve a message id to its session id by scanning the record headers -
    /// resident memory only, never a store read. Newest rows first: recent
    /// messages are the likely targets, and records are laid out in ascending
    /// `row_id` order. Length-check first, so most rows cost one integer
    /// compare. `None` is a miss, including on a corrupt map - an empty or
    /// unresolvable session string must never suppress the store scan.
    pub fn lookup_session_for_message(&self, message_id: &str) -> Option<&str> {
        let needle = message_id.as_bytes();
        for record in self.records().iter().rev() {
            let header = self.header_at(record)?;
            if read_u32(header, 24)? != needle.len() {
                continue;
            }
            let base = self.blob_offset.checked_add(record.blob_off as usize)?;
            let start = base + ROW_HEADER_LEN;
            if self.mmap.get(start..start.checked_add(needle.len())?)? == needle {
                let session_id = self.session_str(read_u32(header, 8)?);
                return (!session_id.is_empty()).then_some(session_id);
            }
        }
        None
    }

    /// Row ids of every `session_id` row in this segment, `Some(empty)` when
    /// the session is not here. `None` on any malformed record: a silently
    /// dropped row would serve an incomplete page with no fallback, so
    /// corruption aborts the map path instead. The session entry's row count
    /// bounds the walk - it stops at the session's last row.
    pub fn session_row_ids(&self, session_id: &str) -> Option<Vec<u64>> {
        let Some(session_idx) = self.session_index(session_id) else {
            return Some(Vec::new());
        };
        let count = self.session_entries()[session_idx].count as usize;
        let mut out = Vec::with_capacity(count);
        for record in self.records() {
            let header = self.header_at(record)?;
            if read_u32(header, 8)? == session_idx {
                out.push(record.row_id);
                if out.len() == count {
                    break;
                }
            }
        }
        Some(out)
    }

    /// Slice `len` UTF-8 bytes at `*at`, advancing `*at`. Checked so a corrupt
    /// map yields `None` (-> take fallback), not a panic.
    fn slice_str(&self, at: &mut usize, len: usize) -> Option<&str> {
        let end = at.checked_add(len)?;
        let bytes = self.mmap.get(*at..end)?;
        *at = end;
        std::str::from_utf8(bytes).ok()
    }
}

/// Streaming encoder for one segment file: rows go in one at a time, in
/// ascending `row_id` order, and the segment is published by [`Self::finish`].
///
/// The point is what it does *not* hold. A row's `search_text` is appended to
/// the open block's plaintext and forgotten; every `BLOCK_ROWS` rows that block
/// is compressed out to a staging temp and the plaintext buffer is reused. The
/// row's blob header goes to a second staging temp as it arrives. So the live
/// set is one block of text plus the dictionaries, not the corpus - the caller
/// can drop each scan batch as soon as it has been pushed.
///
/// Two staging temps rather than one file, because the layout is
/// `blocks | rows | session bytes | dict bytes` and the region before the blob
/// is sized by counts only the completed pass knows: the blob cannot be written
/// at its final offset until the last row has been seen. `finish` computes the
/// fixed region, copies the blocks extent in, then replays the staged rows.
///
/// Dictionary ids are handed out in first-seen order while streaming and
/// remapped to the file's lexical order during that replay - the on-disk bytes
/// are identical to what the buffering path encodes for the same rows.
pub struct RowMetaBuilder {
    target: PathBuf,
    /// Temp the finished segment is written to, then renamed from.
    tmp: PathBuf,
    version: u64,
    blocks: Staging,
    rows: Staging,
    block_entries: Vec<BlockEntry>,
    /// Compressed bytes staged so far - the blob offset of the next block.
    blocks_len: u64,
    /// Plaintext of the block currently being filled.
    plain: Vec<u8>,
    /// Rows in the open block.
    pending: usize,
    /// Staged row-header bytes so far - each row's offset within the row region.
    rows_len: u64,
    /// `blob_off` is row-region-relative until `finish` learns the blocks extent
    /// and shifts every record by it.
    records: Vec<Record>,
    sessions: Interner,
    /// `(message count, max timestamp)` per session, indexed by first-seen id.
    /// The max is the watermark the sync skip oracle compares against the
    /// source's latest message timestamp (spec.md#adapters; deterministic,
    /// rebuilt from the store).
    session_aggs: Vec<(u32, i64)>,
    projects: Interner,
    agents: Interner,
    roles: Interner,
    last_row_id: Option<u64>,
}

impl RowMetaBuilder {
    /// `expected_rows` sizes the record spine up front; it is a hint, and a
    /// stream that runs longer or shorter still encodes correctly.
    pub fn new(path: &Path, version: u64, expected_rows: usize) -> Result<Self> {
        // Unique temp names per builder (pid + nonce): two processes prewarming
        // the same store+version must not share one temp inode, or the second's
        // create would mutate the file the first is mapping. All three carry the
        // `.tmp-` shape `is_orphan_temp` reclaims by, so a crash mid-build
        // leaves nothing a later `sweep_orphan_temps` cannot clean.
        let stamp = format!("tmp-{}-{:016x}", std::process::id(), fastrand::u64(..));
        Ok(Self {
            target: path.to_path_buf(),
            tmp: path.with_extension(&stamp),
            version,
            blocks: Staging::create(path.with_extension(format!("{stamp}-blocks")))?,
            rows: Staging::create(path.with_extension(format!("{stamp}-rows")))?,
            block_entries: Vec::with_capacity(expected_rows.div_ceil(BLOCK_ROWS)),
            blocks_len: 0,
            plain: Vec::new(),
            pending: 0,
            rows_len: 0,
            records: Vec::with_capacity(expected_rows),
            sessions: Interner::default(),
            session_aggs: Vec::new(),
            projects: Interner::default(),
            agents: Interner::default(),
            roles: Interner::default(),
            last_row_id: None,
        })
    }

    /// Fold one row in. Returns [`UnorderedRows`] if `row` goes backwards -
    /// nothing is recoverable at that point, so the caller re-encodes through
    /// the sorting [`RowMetaMap::build`].
    pub fn push(&mut self, row: RowMetaRef<'_>) -> Result<()> {
        if let Some(previous) = self.last_row_id
            && row.row_id < previous
        {
            return Err(UnorderedRows {
                previous,
                current: row.row_id,
            }
            .into());
        }
        self.last_row_id = Some(row.row_id);

        let text_off = u32::try_from(self.plain.len()).context("block too large")?;
        let text_len = u32::try_from(row.search_text.len()).context("search_text too long")?;
        self.plain.extend_from_slice(row.search_text.as_bytes());

        let session = self.sessions.intern(row.session_id);
        // Ids are handed out densely in first-seen order, so a newly interned
        // session is always exactly one past the aggregates seen so far.
        if session as usize == self.session_aggs.len() {
            self.session_aggs.push((0, i64::MIN));
        }
        let agg = &mut self.session_aggs[session as usize];
        agg.0 += 1;
        agg.1 = agg.1.max(row.timestamp_micros);

        let mid_len = u32::try_from(row.message_id.len()).context("message_id too long")?;
        let mut header = [0u8; ROW_HEADER_LEN];
        header[0..8].copy_from_slice(&row.timestamp_micros.to_le_bytes());
        header[8..12].copy_from_slice(&session.to_le_bytes());
        header[12..16].copy_from_slice(&self.projects.intern(row.project).to_le_bytes());
        header[16..20].copy_from_slice(&self.agents.intern(row.source_agent).to_le_bytes());
        header[20..24].copy_from_slice(&self.roles.intern(row.role).to_le_bytes());
        header[24..28].copy_from_slice(&mid_len.to_le_bytes());
        header[28..32].copy_from_slice(&text_off.to_le_bytes());
        header[32..36].copy_from_slice(&text_len.to_le_bytes());
        let writer = self.rows.writer()?;
        writer.write_all(&header)?;
        writer.write_all(row.message_id.as_bytes())?;

        self.records.push(Record {
            row_id: row.row_id,
            blob_off: self.rows_len,
        });
        self.rows_len += ROW_HEADER_LEN as u64 + u64::from(mid_len);

        self.pending += 1;
        if self.pending == BLOCK_ROWS {
            self.seal_block()?;
        }
        Ok(())
    }

    /// Compress the open block out to the staging temp and drop its plaintext.
    fn seal_block(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        let compressed = zstd::bulk::compress(&self.plain, ZSTD_LEVEL).context("zstd compress")?;
        self.block_entries.push(BlockEntry {
            comp_off: self.blocks_len,
            comp_len: u32::try_from(compressed.len()).context("compressed block too large")?,
            decomp_len: u32::try_from(self.plain.len()).context("block too large")?,
        });
        self.blocks.writer()?.write_all(&compressed)?;
        self.blocks_len += compressed.len() as u64;
        // Reused, not freed: the next block regrows a warm buffer.
        self.plain.clear();
        self.pending = 0;
        Ok(())
    }

    /// Assemble the segment and rename it into place.
    pub fn finish(mut self) -> Result<()> {
        self.seal_block()?;

        let (sessions, session_remap) = std::mem::take(&mut self.sessions).into_sorted();
        let (projects, project_remap) = std::mem::take(&mut self.projects).into_sorted();
        let (agents, agent_remap) = std::mem::take(&mut self.agents).into_sorted();
        let (roles, role_remap) = std::mem::take(&mut self.roles).into_sorted();
        // Aggregates follow their session into the file's lexical order.
        let mut session_aggs = vec![(0u32, i64::MIN); sessions.len()];
        for (first_seen, agg) in self.session_aggs.iter().enumerate() {
            session_aggs[session_remap[first_seen] as usize] = *agg;
        }

        let count = self.records.len();
        let blob_offset = (size_of::<Header>()
            + count * size_of::<Record>()
            + sessions.len() * size_of::<SessionEntry>()
            + (projects.len() + agents.len() + roles.len()) * size_of::<DictEntry>()
            + self.block_entries.len() * size_of::<BlockEntry>()) as u64;
        let header = Header {
            magic: MAGIC,
            version: self.version,
            count: count as u64,
            session_count: sessions.len() as u64,
            project_count: projects.len() as u64,
            agent_count: agents.len() as u64,
            role_count: roles.len() as u64,
            block_count: self.block_entries.len() as u64,
            blob_offset,
        };
        // The blocks extent lands first in the blob, so every staged row offset
        // shifts past it.
        for record in &mut self.records {
            record.blob_off += self.blocks_len;
        }

        // A failure below drops `segment` and reclaims the temp with it. The
        // rename stays outside: a build that could not publish must leave one for
        // `sweep_orphan_temps` (`rowmap_purge_probe` pins it).
        let mut segment = Staging::create(self.tmp.clone())?;
        let mut blob_len = self.blocks_len;
        {
            let writer = segment.writer()?;
            writer.seek(SeekFrom::Start(blob_offset))?;

            let mut staged_blocks = self.blocks.rewind()?;
            let copied = std::io::copy(&mut staged_blocks, writer)?;
            ensure!(
                copied == self.blocks_len,
                "row meta map block staging is {copied} bytes, expected {}",
                self.blocks_len
            );

            // Replay the staged rows, remapping first-seen dictionary ids to
            // their lexical index. Everything else about a staged row is already
            // in its final form, so this is a copy with four `u32` patches.
            let mut staged_rows = BufReader::with_capacity(1 << 20, self.rows.rewind()?);
            let mut row_header = [0u8; ROW_HEADER_LEN];
            let mut message_id = Vec::new();
            for _ in 0..count {
                staged_rows.read_exact(&mut row_header)?;
                remap_id(&mut row_header, 8, &session_remap)?;
                remap_id(&mut row_header, 12, &project_remap)?;
                remap_id(&mut row_header, 16, &agent_remap)?;
                remap_id(&mut row_header, 20, &role_remap)?;
                let mid_len = read_u32(&row_header, 24).context("staged row header truncated")?;
                message_id.resize(mid_len, 0);
                staged_rows.read_exact(&mut message_id)?;
                writer.write_all(&row_header)?;
                writer.write_all(&message_id)?;
                blob_len += ROW_HEADER_LEN as u64 + mid_len as u64;
            }
        }

        let writer = segment.writer()?;
        let session_entries = sessions
            .iter()
            .zip(&session_aggs)
            .map(|(sid, (count, max_ts_micros))| {
                let off = blob_len;
                writer.write_all(sid.as_bytes())?;
                let sid_len = u32::try_from(sid.len()).context("session_id too long")?;
                blob_len += u64::from(sid_len);
                Ok(SessionEntry {
                    sid_off: off,
                    max_ts_micros: *max_ts_micros,
                    sid_len,
                    count: *count,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let project_entries = write_dict_entries(writer, &mut blob_len, &projects)?;
        let agent_entries = write_dict_entries(writer, &mut blob_len, &agents)?;
        let role_entries = write_dict_entries(writer, &mut blob_len, &roles)?;

        // The row replay hand-sums ROW_HEADER_LEN, so a field added to the row
        // header would silently shift every offset already recorded.
        #[cfg(debug_assertions)]
        {
            writer.flush()?;
            debug_assert_eq!(
                writer.stream_position()?,
                blob_offset + blob_len,
                "rowmap blob accounting desynced from the bytes actually written",
            );
        }

        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(bytemuck::bytes_of(&header))?;
        writer.write_all(bytemuck::cast_slice(&self.records))?;
        writer.write_all(bytemuck::cast_slice(&session_entries))?;
        writer.write_all(bytemuck::cast_slice(&project_entries))?;
        writer.write_all(bytemuck::cast_slice(&agent_entries))?;
        writer.write_all(bytemuck::cast_slice(&role_entries))?;
        writer.write_all(bytemuck::cast_slice(&self.block_entries))?;
        // Closed before the rename publishes it - this module never renames a
        // path it still holds a handle to (see `sweep_stale_rowmaps` on why
        // Windows file semantics are load-bearing here).
        let tmp = segment.publish()?;
        std::fs::rename(&tmp, &self.target)
            .with_context(|| format!("rename row meta map into place {}", self.target.display()))?;
        Ok(())
    }
}

/// A build temp: streamed into, read back once, and reclaimed on drop unless
/// [`Self::publish`] hands it over.
struct Staging {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
}

impl Staging {
    fn create(path: PathBuf) -> Result<Self> {
        // Read access too: the staging extents are streamed out and then read
        // back through the same handle by `rewind`.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .with_context(|| format!("create row meta map temp {}", path.display()))?;
        Ok(Self {
            path,
            // 1 MiB: the row pass issues two small writes per row, so the default
            // 8 KiB buffer would flush thousands of times on a real corpus.
            writer: Some(BufWriter::with_capacity(1 << 20, file)),
        })
    }

    fn writer(&mut self) -> Result<&mut BufWriter<File>> {
        self.writer
            .as_mut()
            .context("row meta map temp is already closed")
    }

    /// Close the writer and hand back the file positioned to be read from the
    /// start. The path stays owned, so dropping the `Staging` still reclaims it.
    fn rewind(&mut self) -> Result<File> {
        let mut file = self.take_file()?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }

    /// Flush, fsync, close, and give up ownership of the path: the caller is
    /// publishing the file, so drop must no longer reclaim it.
    fn publish(&mut self) -> Result<PathBuf> {
        let file = self.take_file()?;
        file.sync_all()?;
        drop(file);
        Ok(std::mem::take(&mut self.path))
    }

    fn take_file(&mut self) -> Result<File> {
        self.writer
            .take()
            .context("row meta map temp is already closed")?
            .into_inner()
            .map_err(|err| err.into_error())
            .context("flush row meta map temp")
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        // Closed before the unlink: Windows refuses to delete an open file.
        self.writer.take();
        if !self.path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// First-seen dictionary interner: one owned copy per distinct value, dense
/// `u32` ids in arrival order, remapped to the file's lexical order at the end.
/// Scratch is proportional to cardinality, not to rows.
#[derive(Default)]
struct Interner {
    ids: HashMap<Arc<str>, u32>,
    values: Vec<Arc<str>>,
}

impl Interner {
    fn intern(&mut self, value: &str) -> u32 {
        if let Some(id) = self.ids.get(value) {
            return *id;
        }
        let value: Arc<str> = Arc::from(value);
        let id = self.values.len() as u32;
        self.values.push(Arc::clone(&value));
        self.ids.insert(value, id);
        id
    }

    /// `(values in lexical order, first-seen id -> lexical index)`.
    fn into_sorted(self) -> (Vec<Arc<str>>, Vec<u32>) {
        let mut order: Vec<u32> = (0..self.values.len() as u32).collect();
        order.sort_unstable_by(|left, right| {
            self.values[*left as usize].cmp(&self.values[*right as usize])
        });
        let mut remap = vec![0u32; self.values.len()];
        let mut values = Vec::with_capacity(self.values.len());
        for (index, first_seen) in order.into_iter().enumerate() {
            remap[first_seen as usize] = index as u32;
            values.push(Arc::clone(&self.values[first_seen as usize]));
        }
        (values, remap)
    }
}

/// Rewrite the `u32` at `at` in a staged row header through `remap`.
fn remap_id(header: &mut [u8; ROW_HEADER_LEN], at: usize, remap: &[u32]) -> Result<()> {
    let id = read_u32(header, at).context("staged row header truncated")?;
    let mapped = remap.get(id).context("staged dictionary id out of range")?;
    header[at..at + 4].copy_from_slice(&mapped.to_le_bytes());
    Ok(())
}

/// The on-disk LSM chain for a store: the highest-version base plus every
/// delta layered above it, ascending.
pub struct ChainPaths {
    pub base: PathBuf,
    pub base_version: u64,
    pub deltas: Vec<(u64, PathBuf)>,
}

impl ChainPaths {
    /// Version the chain covers - the newest segment's version.
    pub fn version(&self) -> u64 {
        self.deltas
            .last()
            .map(|(version, _)| *version)
            .unwrap_or(self.base_version)
    }
}

/// Does `file_name` name an abandoned build temp for `store_key`?
///
/// One definition, two enforcement points: `Store::sweep_orphan_temps` reclaims
/// by it, and the rowmap purge probe asserts that a rebuild which could not
/// rename leaves behind something it matches. Held here, beside the
/// `with_extension("tmp-{pid}-{nonce}")` in `RowMetaBuilder::new` - which names
/// the segment temp and both staging extents - because that is what makes the
/// shape true; duplicating the predicate let the sweep change while the probe
/// kept passing on a rule that no longer held.
pub fn is_orphan_temp(file_name: &str, store_key: &str) -> bool {
    file_name.starts_with(&format!("rowmetamap-{store_key}-")) && file_name.contains(".tmp-")
}

/// Discover the chain under `cache_dir` for `store_key`: the highest-version
/// base (`-v{V}`) plus every delta (`-d{V}`) above it, ascending. `None` if no
/// base exists yet.
pub fn discover_chain(cache_dir: &Path, store_key: &str) -> Option<ChainPaths> {
    let prefix = format!("rowmetamap-{store_key}-");
    let mut bases: Vec<(u64, PathBuf)> = Vec::new();
    let mut deltas: Vec<(u64, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(cache_dir).ok()?.flatten() {
        let name = entry.file_name();
        let Some(rest) = name
            .to_str()
            .and_then(|name| name.strip_prefix(&prefix))
            .and_then(|rest| rest.strip_suffix(".rmm"))
        else {
            continue;
        };
        if let Some(version) = rest.strip_prefix('v').and_then(|d| d.parse::<u64>().ok()) {
            bases.push((version, entry.path()));
        } else if let Some(version) = rest.strip_prefix('d').and_then(|d| d.parse::<u64>().ok()) {
            deltas.push((version, entry.path()));
        }
    }
    let (base_version, base) = bases.into_iter().max_by_key(|(version, _)| *version)?;
    let mut deltas: Vec<(u64, PathBuf)> = deltas
        .into_iter()
        .filter(|(version, _)| *version > base_version)
        .collect();
    deltas.sort_by_key(|(version, _)| *version);
    Some(ChainPaths {
        base,
        base_version,
        deltas,
    })
}

/// An LSM chain of immutable segment maps (base + ascending deltas) viewed as
/// one logical map. Rows are partitioned across segments by `row_id` (append is
/// disjoint; compaction rebuilds the base), so key/meta lookups take the newest
/// hit and counts sum across segments.
pub struct RowMetaSet {
    segments: Vec<RowMetaMap>,
}

impl std::fmt::Debug for RowMetaSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowMetaSet")
            .field("segments", &self.segments.len())
            .field("version", &self.version())
            .finish()
    }
}

impl RowMetaSet {
    /// Open every segment in `paths` (base first, then deltas ascending).
    pub fn open(paths: &ChainPaths) -> Result<Self> {
        let mut segments = Vec::with_capacity(1 + paths.deltas.len());
        segments.push(RowMetaMap::open(&paths.base)?);
        for (_, delta) in &paths.deltas {
            segments.push(RowMetaMap::open(delta)?);
        }
        Ok(Self { segments })
    }

    pub fn version(&self) -> u64 {
        self.segments
            .iter()
            .map(RowMetaMap::version)
            .max()
            .unwrap_or(0)
    }

    /// Number of delta segments layered on the base.
    pub fn delta_count(&self) -> usize {
        self.segments.len().saturating_sub(1)
    }

    /// No rows in any segment - the first-ingest hint that lets the sync oracle
    /// short-circuit the per-session source last-id read.
    pub fn is_empty(&self) -> bool {
        self.segments.iter().all(RowMetaMap::is_empty)
    }

    /// Total row entries across every segment. Rows are disjoint across segments
    /// (append-only deltas), so this sums - the live row count the base covers.
    pub fn len(&self) -> usize {
        self.segments.iter().map(RowMetaMap::len).sum()
    }

    /// Highest `row_id` across all segments - the append high-water mark a delta
    /// extends past. Stable row ids keep it monotonic under fragment churn.
    pub fn max_row_id(&self) -> Option<u64> {
        self.segments
            .iter()
            .filter_map(RowMetaMap::max_row_id)
            .max()
    }

    /// Newest segment wins (a row lives in exactly one segment).
    pub fn lookup(&self, row_id: u64) -> Option<(&str, &str)> {
        self.segments
            .iter()
            .rev()
            .find_map(|seg| seg.lookup(row_id))
    }

    /// Hydrate `rowids` to owned metas, splitting out the ones no segment holds
    /// (appended since the build) for the caller's take_rows fallback. Output
    /// order is unspecified (caller indexes by key). Rowids are visited in
    /// sorted order with a per-segment block cache, so the common case - many
    /// hits from a few row-id-adjacent sessions - decompresses each block once.
    pub fn hydrate(&self, rowids: &[u64]) -> (Vec<RowMetaEntry>, Vec<u64>) {
        let mut sorted = rowids.to_vec();
        sorted.sort_unstable();
        let mut caches: Vec<BlockCache> = vec![None; self.segments.len()];
        let mut hits = Vec::with_capacity(sorted.len());
        let mut misses = Vec::new();
        for row_id in sorted {
            let hit = self
                .segments
                .iter()
                .enumerate()
                .rev()
                .find_map(|(segment, map)| {
                    let meta = map.lookup_meta(row_id, &mut caches[segment])?;
                    Some(RowMetaEntry {
                        row_id,
                        session_id: meta.session_id.to_owned(),
                        message_id: meta.message_id.to_owned(),
                        role: meta.role.to_owned(),
                        project: meta.project.to_owned(),
                        source_agent: meta.source_agent.to_owned(),
                        timestamp_micros: meta.timestamp_micros,
                        search_text: meta.search_text,
                    })
                });
            match hit {
                Some(entry) => hits.push(entry),
                None => misses.push(row_id),
            }
        }
        (hits, misses)
    }

    /// A session's rows are split across segments, so its count is the sum.
    pub fn lookup_count(&self, session_id: &str) -> Option<usize> {
        let mut total = 0;
        let mut found = false;
        for seg in &self.segments {
            if let Some(count) = seg.lookup_count(session_id) {
                total += count;
                found = true;
            }
        }
        found.then_some(total)
    }

    /// Max message timestamp (micros) for `session_id` across the chain - the max
    /// over every segment that holds it (a session's rows can be split across
    /// base and deltas). `None` if no segment has it.
    pub fn lookup_max_ts(&self, session_id: &str) -> Option<i64> {
        self.segments
            .iter()
            .filter_map(|seg| seg.lookup_max_ts(session_id))
            .max()
    }

    pub fn session_watermarks(&self) -> std::collections::BTreeMap<String, i64> {
        let mut watermarks: std::collections::BTreeMap<String, i64> =
            std::collections::BTreeMap::new();
        for (session_id, timestamp) in self
            .segments
            .iter()
            .flat_map(RowMetaMap::session_watermarks)
        {
            watermarks
                .entry(session_id.to_owned())
                .and_modify(|stored| *stored = (*stored).max(timestamp))
                .or_insert(timestamp);
        }
        watermarks
    }

    /// Resolve a message id to its session id, newest segment first (recent
    /// messages - the likely targets - live in the small deltas). `None` is a
    /// miss the caller resolves against the store.
    pub fn lookup_session_for_message(&self, message_id: &str) -> Option<&str> {
        self.segments
            .iter()
            .rev()
            .find_map(|seg| seg.lookup_session_for_message(message_id))
    }

    /// A session's row ids across the chain (rows are disjoint across
    /// segments, so this concatenates). `None` if any segment reports a
    /// malformed record - the caller falls back to the store.
    pub fn session_row_ids(&self, session_id: &str) -> Option<Vec<u64>> {
        let mut out = Vec::new();
        for seg in &self.segments {
            out.extend(seg.session_row_ids(session_id)?);
        }
        Some(out)
    }

    /// Encode every row of the chain plus `appended` into a fresh base segment
    /// at `path` - the compaction rebuild, which never re-reads the store.
    ///
    /// Segments are already `row_id`-sorted, so this is a k-way merge over their
    /// records plus the sorted `appended` rows, pushed straight into
    /// [`RowMetaBuilder`]. The corpus is never reconstructed as owned entries:
    /// each row is a borrow into its segment's mapping and its one decompressed
    /// text block. On a `row_id` collision the newest source wins - `appended`
    /// over every segment, later segments over earlier ones, and the last of a
    /// run of duplicates within `appended` - and the superseded rows are
    /// skipped, so a row id appears exactly once.
    ///
    /// A segment whose records are not `row_id`-sorted is corrupt (its binary
    /// search is already wrong) and breaks the merge. That falls back to a
    /// sorting rebuild rather than erroring: the pre-streaming build sorted, so
    /// such a chain self-healed on the next compaction, and returning an error
    /// here would instead fail every later `ensure_rowmap` on the same chain
    /// forever.
    pub fn compact_into(
        &self,
        path: &Path,
        version: u64,
        mut appended: Vec<RowMetaEntry>,
    ) -> Result<()> {
        // Stable, so a duplicated row_id keeps its arrival order and the last
        // one really is the newest.
        appended.sort_by_key(|entry| entry.row_id);
        match self.merge_into(path, version, &appended) {
            Err(error) if error.downcast_ref::<UnorderedRows>().is_some() => {
                tracing::warn!(
                    %error,
                    "rowmap segment records are not row_id-ordered; compacting through a sorting rebuild"
                );
                self.sorted_rebuild(path, version, appended)
            }
            result => result,
        }
    }

    /// The streaming k-way merge itself. `appended` must be sorted by `row_id`.
    fn merge_into(&self, path: &Path, version: u64, appended: &[RowMetaEntry]) -> Result<()> {
        let mut cursors: Vec<SegmentCursor<'_>> =
            self.segments.iter().map(SegmentCursor::new).collect();
        let mut next_appended = 0usize;
        let mut dropped = 0usize;
        // An upper bound, not a count: the spine shrinks by however many row ids
        // the merge collapses.
        let mut builder = RowMetaBuilder::new(path, version, self.len() + appended.len())?;
        loop {
            let mut row_id = appended.get(next_appended).map(|entry| entry.row_id);
            for cursor in &cursors {
                if let Some(candidate) = cursor.peek() {
                    row_id = Some(row_id.map_or(candidate, |current| current.min(candidate)));
                }
            }
            let Some(row_id) = row_id else { break };

            let from_appended = appended
                .get(next_appended)
                .is_some_and(|entry| entry.row_id == row_id);
            if from_appended {
                // Newest wins inside `appended` too: skip to the last entry of
                // this row id's run instead of emitting a record per duplicate.
                while appended
                    .get(next_appended + 1)
                    .is_some_and(|entry| entry.row_id == row_id)
                {
                    next_appended += 1;
                }
                builder.push(appended[next_appended].as_row())?;
                next_appended += 1;
            } else if let Some(newest) = cursors
                .iter()
                .rposition(|cursor| cursor.peek() == Some(row_id))
            {
                cursors[newest].load_block();
                match cursors[newest].row() {
                    Some(row) => builder.push(row)?,
                    // A record whose blob offsets do not resolve: the rebuilt
                    // base is short that row, and a caller reading it can no
                    // longer tell. Loud, because a base that silently loses rows
                    // outlives every read that would have caught it.
                    None => dropped += 1,
                }
            }

            for cursor in &mut cursors {
                if cursor.peek() == Some(row_id) {
                    cursor.advance();
                }
            }
        }
        if dropped > 0 {
            tracing::warn!(
                dropped,
                path = %path.display(),
                "rowmap compaction could not read some segment records; the rebuilt base is short that many rows"
            );
        }
        builder.finish()
    }

    /// Re-encode the whole chain plus `appended` through the sorting
    /// [`RowMetaMap::build`] - what the pre-streaming compaction did. Holds the
    /// corpus as owned entries, so it is the corruption path only.
    fn sorted_rebuild(&self, path: &Path, version: u64, appended: Vec<RowMetaEntry>) -> Result<()> {
        let mut merged: HashMap<u64, RowMetaEntry> = HashMap::with_capacity(self.len());
        // Base first, deltas ascending, `appended` last: the later insert for a
        // row id overwrites the earlier, which is the same newest-wins order the
        // merge applies.
        for segment in &self.segments {
            let mut cursor = SegmentCursor::new(segment);
            while cursor.peek().is_some() {
                cursor.load_block();
                if let Some(row) = cursor.row() {
                    merged.insert(row.row_id, row.to_entry());
                }
                cursor.advance();
            }
        }
        for entry in appended {
            merged.insert(entry.row_id, entry);
        }
        RowMetaMap::build(path, version, merged.into_values().collect())
    }
}

/// Sequential reader over one segment's records, decompressing each text block
/// once as it walks - the merge's view of an already `row_id`-sorted segment.
struct SegmentCursor<'a> {
    map: &'a RowMetaMap,
    next: usize,
    loaded_block: Option<usize>,
    plain: Vec<u8>,
}

impl<'a> SegmentCursor<'a> {
    fn new(map: &'a RowMetaMap) -> Self {
        Self {
            map,
            next: 0,
            loaded_block: None,
            plain: Vec::new(),
        }
    }

    fn peek(&self) -> Option<u64> {
        self.map
            .records()
            .get(self.next)
            .map(|record| record.row_id)
    }

    /// Records are visited in order, so the block holding the current row is
    /// decompressed once for the whole run of rows that share it.
    fn load_block(&mut self) {
        let block = self.next / BLOCK_ROWS;
        if self.loaded_block != Some(block) {
            self.plain = self.map.decompress_block(block).unwrap_or_default();
            self.loaded_block = Some(block);
        }
    }

    fn row(&self) -> Option<RowMetaRef<'_>> {
        self.map.row_ref(self.next, &self.plain)
    }

    fn advance(&mut self) {
        self.next += 1;
    }
}

fn write_dict_entries(
    writer: &mut impl Write,
    blob_len: &mut u64,
    values: &[Arc<str>],
) -> Result<Vec<DictEntry>> {
    values
        .iter()
        .map(|value| {
            let off = *blob_len;
            writer.write_all(value.as_bytes())?;
            let len = u32::try_from(value.len()).context("dictionary value too long")?;
            *blob_len += u64::from(len);
            Ok(DictEntry { off, len, _pad: 0 })
        })
        .collect()
}

fn read_u32(bytes: &[u8], at: usize) -> Option<usize> {
    let slice = bytes.get(at..at.checked_add(4)?)?;
    Some(u32::from_le_bytes(slice.try_into().ok()?) as usize)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    fn entry(
        row_id: u64,
        session_id: &str,
        message_id: &str,
        timestamp_micros: i64,
        search_text: &str,
    ) -> RowMetaEntry {
        RowMetaEntry {
            row_id,
            session_id: session_id.to_owned(),
            message_id: message_id.to_owned(),
            role: "user".to_owned(),
            project: "/proj".to_owned(),
            source_agent: "claude-code".to_owned(),
            timestamp_micros,
            search_text: search_text.to_owned(),
        }
    }

    #[test]
    fn message_and_conversational_lookups_cover_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        let base_path = RowMetaMap::path_for(dir.path(), "s", 1);
        RowMetaMap::build(
            &base_path,
            1,
            vec![
                entry(1, "sess-a", "msg-1", 1_000, "hello"),
                entry(2, "sess-a", "msg-2", 2_000, ""), // bare tool call: not conversational
                entry(3, "sess-b", "msg-3", 3_000, "there"),
            ],
        )
        .unwrap();
        let delta_path = RowMetaMap::delta_path(dir.path(), "s", 2);
        RowMetaMap::build(
            &delta_path,
            2,
            vec![entry(9, "sess-a", "msg-9", 9_000, "newest")],
        )
        .unwrap();
        let set = RowMetaSet::open(&ChainPaths {
            base: base_path,
            base_version: 1,
            deltas: vec![(2, delta_path)],
        })
        .unwrap();

        assert_eq!(set.lookup_session_for_message("msg-1"), Some("sess-a"));
        assert_eq!(
            set.lookup_session_for_message("msg-9"),
            Some("sess-a"),
            "delta hit"
        );
        assert_eq!(set.lookup_session_for_message("msg-3"), Some("sess-b"));
        assert_eq!(set.lookup_session_for_message("absent"), None);

        let mut ids = set.session_row_ids("sess-a").expect("intact map");
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 9], "all roles, base and delta");
        assert_eq!(
            set.session_row_ids("missing").expect("intact map"),
            Vec::<u64>::new(),
            "absent session is empty, not a corruption signal"
        );
    }

    #[test]
    fn build_open_lookup_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = RowMetaMap::path_for(dir.path(), "teststore", 7);
        let mut three = entry(99, "sess-a", "msg-3", 3_000, "third");
        three.role = "assistant".to_owned();
        three.project = "/other".to_owned();
        let entries = vec![
            entry(10, "sess-a", "msg-1", 1_000, "first message text"),
            entry(3, "sess-b/agent-x", "msg-2", 2_000, ""),
            three,
        ];
        RowMetaMap::build(&path, 7, entries).unwrap();

        let map = RowMetaMap::open(&path).unwrap();
        assert_eq!(map.version(), 7);
        assert_eq!(map.len(), 3);
        assert_eq!(map.lookup(10), Some(("sess-a", "msg-1")));
        assert_eq!(map.lookup(3), Some(("sess-b/agent-x", "msg-2")));
        assert_eq!(map.lookup(99), Some(("sess-a", "msg-3")));
        assert_eq!(map.lookup(42), None);

        let meta = map.lookup_meta(10, &mut None).expect("row 10 present");
        assert_eq!(meta.session_id, "sess-a");
        assert_eq!(meta.message_id, "msg-1");
        assert_eq!(meta.role, "user");
        assert_eq!(meta.project, "/proj");
        assert_eq!(meta.source_agent, "claude-code");
        assert_eq!(meta.timestamp_micros, 1_000);
        assert_eq!(meta.search_text, "first message text");

        let assistant = map.lookup_meta(99, &mut None).expect("row 99 present");
        assert_eq!(assistant.role, "assistant");
        assert_eq!(assistant.project, "/other");
        assert_eq!(assistant.search_text, "third");

        let empty_text = map.lookup_meta(3, &mut None).expect("row 3 present");
        assert_eq!(empty_text.search_text, "");
        assert!(map.lookup_meta(42, &mut None).is_none());

        assert_eq!(map.lookup_count("sess-a"), Some(2));
        assert_eq!(map.lookup_count("sess-b/agent-x"), Some(1));
        assert_eq!(map.lookup_count("missing"), None);

        // Watermark = max timestamp: sess-a's msg-3 (ts 3000) over msg-1.
        assert_eq!(map.lookup_max_ts("sess-a"), Some(3_000));
        assert_eq!(map.lookup_max_ts("sess-b/agent-x"), Some(2_000));
        assert_eq!(map.lookup_max_ts("missing"), None);
    }

    #[test]
    fn max_ts_is_the_session_high_water_mark() {
        let dir = tempfile::tempdir().unwrap();
        let path = RowMetaMap::path_for(dir.path(), "ts", 1);
        // Out-of-row-order timestamps: the max wins regardless of row order.
        let entries = vec![
            entry(1, "s", "msg-a", 5_000, "a"),
            entry(2, "s", "msg-b", 9_000, "b"),
            entry(3, "s", "msg-c", 7_000, "c"),
        ];
        RowMetaMap::build(&path, 1, entries).unwrap();
        let map = RowMetaMap::open(&path).unwrap();
        assert_eq!(map.lookup_max_ts("s"), Some(9_000));
    }

    #[test]
    fn many_blocks_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = RowMetaMap::path_for(dir.path(), "blocks", 1);
        let entries: Vec<RowMetaEntry> = (0..(BLOCK_ROWS as u64 * 2 + 5))
            .map(|i| {
                entry(
                    i,
                    "sess",
                    &format!("msg-{i}"),
                    i as i64,
                    &format!("text body {i}"),
                )
            })
            .collect();
        RowMetaMap::build(&path, 1, entries).unwrap();

        let map = RowMetaMap::open(&path).unwrap();
        // One cache reused across rows spanning several blocks - same-block hits
        // must reuse it, block crossings must refill it, both yielding the right
        // text.
        let mut cache = None;
        for i in [0u64, 1, 255, 256, 257, 511, 512, 516] {
            let meta = map.lookup_meta(i, &mut cache).expect("row present");
            assert_eq!(meta.message_id, format!("msg-{i}"));
            assert_eq!(meta.search_text, format!("text body {i}"));
        }
    }

    #[test]
    fn lsm_set_layers_delta_over_base() {
        let dir = tempfile::tempdir().unwrap();
        let base = vec![
            entry(10, "sess-a", "m10", 1, "base ten"),
            entry(11, "sess-a", "m11", 2, "base eleven"),
            entry(12, "sess-b", "m12", 3, "base twelve"),
        ];
        RowMetaMap::build(&RowMetaMap::path_for(dir.path(), "k", 1), 1, base).unwrap();
        let delta = vec![
            entry(20, "sess-a", "m20", 4, "delta twenty"),
            entry(21, "sess-c", "m21", 5, "delta twentyone"),
        ];
        RowMetaMap::build(&RowMetaMap::delta_path(dir.path(), "k", 2), 2, delta).unwrap();

        let chain = discover_chain(dir.path(), "k").expect("chain present");
        assert_eq!(chain.base_version, 1);
        assert_eq!(chain.deltas.len(), 1);
        assert_eq!(chain.version(), 2);

        let set = RowMetaSet::open(&chain).unwrap();
        assert_eq!(set.version(), 2);
        assert_eq!(set.delta_count(), 1);

        assert_eq!(set.lookup(10), Some(("sess-a", "m10")));
        assert_eq!(set.lookup(20), Some(("sess-a", "m20")));
        assert_eq!(set.lookup(99), None);

        // hydrate spans both segments and splits out the absent row.
        let (mut hits, misses) = set.hydrate(&[21, 10, 99]);
        assert_eq!(misses, vec![99]);
        hits.sort_by_key(|entry| entry.row_id);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].search_text, "base ten");
        assert_eq!(hits[1].search_text, "delta twentyone");

        // Counts sum across base + delta: sess-a = 2 + 1.
        assert_eq!(set.lookup_count("sess-a"), Some(3));
        assert_eq!(set.lookup_count("sess-b"), Some(1));
        assert_eq!(set.lookup_count("sess-c"), Some(1));
        assert_eq!(set.lookup_count("missing"), None);

        // Max timestamp across segments: sess-a spans base (ts<=2) + delta (ts 4).
        assert_eq!(set.lookup_max_ts("sess-a"), Some(4));
        assert_eq!(set.lookup_max_ts("sess-b"), Some(3));
        assert_eq!(set.lookup_max_ts("sess-c"), Some(5));
        assert_eq!(set.lookup_max_ts("missing"), None);

        // Compaction: the whole chain merged into one fresh base.
        let compacted_path = RowMetaMap::path_for(dir.path(), "k", 3);
        set.compact_into(&compacted_path, 3, Vec::new()).unwrap();
        let compacted = RowMetaMap::open(&compacted_path).unwrap();
        assert_eq!(compacted.len(), 5, "all 5 distinct rows carried over");
        assert_eq!(compacted.max_row_id(), Some(21));
        assert_eq!(
            compacted
                .lookup_meta(21, &mut None)
                .expect("row 21 present")
                .search_text,
            "delta twentyone"
        );
        assert_eq!(compacted.lookup_count("sess-a"), Some(3));
        assert_eq!(compacted.lookup_max_ts("sess-c"), Some(5));
    }

    /// Compaction reads its rows out of the mmap'd chain instead of rebuilding
    /// them as owned entries, so it gets its own byte-compat guard: the segment
    /// it writes must equal what the buffering build encodes from the same rows.
    /// The delta here overlaps the base on `row_id` 12 and appends past it, so
    /// the newest-wins collision rule is part of what is compared.
    #[test]
    fn compaction_encodes_what_the_buffered_build_would() {
        let dir = tempfile::tempdir().unwrap();
        let base: Vec<RowMetaEntry> = (0..(BLOCK_ROWS as u64 + 9))
            .map(|i| {
                entry(
                    i,
                    &format!("sess-{}", i % 5),
                    &format!("m{i}"),
                    i as i64,
                    &format!("base row {i}"),
                )
            })
            .collect();
        let mut delta: Vec<RowMetaEntry> = (0..7u64)
            .map(|i| {
                entry(
                    300 + i,
                    &format!("sess-{}", i % 3),
                    &format!("d{i}"),
                    500 + i as i64,
                    &format!("delta row {i}"),
                )
            })
            .collect();
        // A row the base already holds: the newer segment must win.
        delta.push(entry(
            12,
            "sess-new",
            "m12-rewritten",
            900,
            "rewritten twelve",
        ));
        delta.sort_unstable_by_key(|entry| entry.row_id);
        let appended = vec![entry(400, "sess-9", "a400", 1_000, "appended four hundred")];

        RowMetaMap::build(&RowMetaMap::path_for(dir.path(), "c", 1), 1, base.clone()).unwrap();
        RowMetaMap::build(
            &RowMetaMap::delta_path(dir.path(), "c", 2),
            2,
            delta.clone(),
        )
        .unwrap();
        let chain = discover_chain(dir.path(), "c").expect("chain present");
        let set = RowMetaSet::open(&chain).unwrap();

        let compacted = dir.path().join("compacted.rmm");
        set.compact_into(&compacted, 3, appended.clone()).unwrap();

        // The same rows collapsed by hand, newest source last.
        let mut expected_rows: HashMap<u64, RowMetaEntry> = HashMap::new();
        for entry in base.into_iter().chain(delta).chain(appended) {
            expected_rows.insert(entry.row_id, entry);
        }
        let expected = dir.path().join("expected.rmm");
        RowMetaMap::build(&expected, 3, expected_rows.into_values().collect()).unwrap();

        assert_eq!(
            digest(&compacted),
            digest(&expected),
            "the merged compaction and the buffered build must encode the same bytes"
        );
    }

    #[test]
    fn build_writes_a_dense_file_and_leaves_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let path = RowMetaMap::path_for(dir.path(), "dense", 1);
        let entries: Vec<RowMetaEntry> = (0..(BLOCK_ROWS as u64 * 2 + 3))
            .map(|i| {
                entry(
                    i,
                    "sess",
                    &format!("msg-{i}"),
                    i as i64,
                    &format!("body {i}"),
                )
            })
            .collect();
        RowMetaMap::build(&path, 1, entries).unwrap();

        let map = RowMetaMap::open(&path).unwrap();
        // The role dictionary is the last thing the blob pass writes, so its
        // final value ends exactly at the blob's end. A seek-back pass that
        // wrote fewer than blob_offset bytes would leave a zero hole, and a
        // mis-summed blob_len would leave slack - both show up as a length
        // mismatch here.
        let blob_len = map
            .dict_entries(map.roles_off, map.role_count)
            .last()
            .map(|dict| dict.off + u64::from(dict.len))
            .expect("role dictionary is non-empty");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            map.blob_offset as u64 + blob_len,
            "built file must hold exactly the header region plus the blob",
        );

        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
            .filter(|name| is_orphan_temp(name, "dense"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "build left a temp behind: {leftovers:?}"
        );
    }

    /// A deliberately awkward corpus for the byte-compat guards: three blocks
    /// with a partial last one, duplicate-key runs (session, project, agent) of
    /// 7, 5 and 13 rows - lengths coprime with `BLOCK_ROWS`, so every run
    /// straddles both block boundaries - empty and non-ASCII `search_text`, and
    /// project/agent/role dictionary values whose first-seen order differs from
    /// their lexical order. `role` cycles every row, so it is the run-length-1
    /// case rather than a straddling run. The session dictionary is deliberately
    /// first-seen == lexical here so the pinned digest stays legible; the
    /// non-identity session remap is covered by
    /// [`remapped_session_dictionary_matches_the_buffered_build`]. Returned in
    /// ascending `row_id` order - the streaming build's required input order.
    fn compat_fixture() -> Vec<RowMetaEntry> {
        let roles = ["user", "assistant", "system"];
        let projects = ["/z/last", "/a/first", "/m/middle"];
        let agents = ["opencode", "claude-code", "codex"];
        (0..(BLOCK_ROWS as u64 * 2 + 37))
            .map(|i| {
                let text = match i % 11 {
                    0 => String::new(),
                    3 => format!("ünïcode ✓ body {i}"),
                    _ => format!("message body {i} with some repeated filler filler filler"),
                };
                RowMetaEntry {
                    row_id: i * 3 + 1,
                    session_id: format!("sess-{:04}", i / 7),
                    message_id: format!("msg-{i:08}"),
                    role: roles[(i % 3) as usize].to_owned(),
                    project: projects[(i / 5 % 3) as usize].to_owned(),
                    source_agent: agents[(i / 13 % 3) as usize].to_owned(),
                    timestamp_micros: 1_700_000_000_000_000 + (i as i64 % 97) * 1_000,
                    search_text: text,
                }
            })
            .collect()
    }

    fn digest(path: &Path) -> String {
        use sha2::{Digest, Sha256};
        let bytes = std::fs::read(path).unwrap();
        format!("{:x}", Sha256::digest(&bytes))
    }

    /// The published layout is a compatibility surface: a map built by one pond
    /// is opened by another (and by an older binary still running). This pins
    /// the exact bytes `compat_fixture` encodes to, so a refactor of the build
    /// path - the chunked scan window, a reordered pass, a different scratch
    /// structure - cannot silently change what lands on disk.
    ///
    /// A zstd upgrade that changes its output for the same input is the one
    /// legitimate way to break this. Recompute the digest then (and only then),
    /// and treat it as a format revision.
    #[test]
    fn build_output_is_byte_stable() {
        const EXPECTED: &str = "0f103bfcf49fdb9f1f3e5c697ec64557cd63d5e516fa939daed0b0b4a13949de";
        let dir = tempfile::tempdir().unwrap();
        let sorted = compat_fixture();

        let from_sorted = dir.path().join("sorted.rmm");
        RowMetaMap::build(&from_sorted, 42, sorted.clone()).unwrap();

        // Same rows, scrambled: the build sorts by row_id, so the bytes must not
        // depend on the caller's order.
        let mut scrambled = sorted;
        let len = scrambled.len();
        for i in 0..len / 2 {
            scrambled.swap(i, len - 1 - i * 2 % len);
        }
        let from_scrambled = dir.path().join("scrambled.rmm");
        RowMetaMap::build(&from_scrambled, 42, scrambled).unwrap();

        assert_eq!(
            digest(&from_sorted),
            digest(&from_scrambled),
            "input order must not change the encoded bytes"
        );
        assert_eq!(digest(&from_sorted), EXPECTED, "on-disk rowmap bytes moved");
    }

    /// The streaming path must encode exactly what the buffering path does, and
    /// must not care where the caller's chunk boundaries fall. The fixture's
    /// duplicate-key runs are 5, 7 and 13 rows long, so every chunk size below
    /// splits some run - including in the middle of a session and across the
    /// 256-row block boundary - and dictionary values are first seen in one
    /// chunk and reused in the next.
    #[test]
    fn chunked_pushes_match_the_buffered_build() {
        let dir = tempfile::tempdir().unwrap();
        let rows = compat_fixture();
        let buffered = dir.path().join("buffered.rmm");
        RowMetaMap::build(&buffered, 42, rows.clone()).unwrap();
        let expected = digest(&buffered);

        for chunk in [1usize, 5, 7, 64, 255, 256, 257, 300, 1000] {
            let path = dir.path().join(format!("chunked-{chunk}.rmm"));
            let mut builder = RowMetaBuilder::new(&path, 42, rows.len()).unwrap();
            for batch in rows.chunks(chunk) {
                // Owned per chunk and dropped at the end of the iteration: a
                // batch the builder still needed would fail here, not silently
                // work because the caller kept the corpus alive.
                let batch: Vec<RowMetaEntry> = batch.to_vec();
                for entry in &batch {
                    builder.push(entry.as_row()).unwrap();
                }
            }
            builder.finish().unwrap();
            assert_eq!(
                digest(&path),
                expected,
                "chunk size {chunk} changed the bytes"
            );
        }
    }

    /// `compat_fixture`'s session ids are first-seen in lexical order, so the
    /// pinned digest never exercises a non-identity session remap: the staged
    /// row headers' session ids and the `session_aggs` reorder both happen to be
    /// the identity there. This runs the same guards over a fixture whose
    /// session first-seen order is the reverse of its lexical order, so every
    /// session id in every staged row header has to be rewritten and every
    /// aggregate has to move.
    #[test]
    fn remapped_session_dictionary_matches_the_buffered_build() {
        let dir = tempfile::tempdir().unwrap();
        let sessions = (BLOCK_ROWS as u64 * 2 + 37).div_ceil(7);
        // Descending session ids: `sess-0000` is first seen last.
        let rows: Vec<RowMetaEntry> = compat_fixture()
            .into_iter()
            .enumerate()
            .map(|(i, mut row)| {
                row.session_id = format!("sess-{:04}", sessions - 1 - i as u64 / 7);
                row
            })
            .collect();
        assert!(
            rows[0].session_id > rows[rows.len() - 1].session_id,
            "the fixture must see its sessions in reverse lexical order",
        );

        let buffered = dir.path().join("buffered.rmm");
        RowMetaMap::build(&buffered, 42, rows.clone()).unwrap();
        let expected = digest(&buffered);

        for chunk in [1usize, 7, 256, 1000] {
            let path = dir.path().join(format!("chunked-{chunk}.rmm"));
            let mut builder = RowMetaBuilder::new(&path, 42, rows.len()).unwrap();
            for batch in rows.chunks(chunk) {
                let batch: Vec<RowMetaEntry> = batch.to_vec();
                for row in &batch {
                    builder.push(row.as_row()).unwrap();
                }
            }
            builder.finish().unwrap();
            assert_eq!(
                digest(&path),
                expected,
                "chunk size {chunk} changed the bytes under a remapped session dictionary"
            );
        }

        // Functional too, not just byte-equal: a remap that lost track of which
        // aggregate belonged to which session would still digest-match itself.
        let map = RowMetaMap::open(&buffered).unwrap();
        let mut expected_counts: HashMap<&str, usize> = HashMap::new();
        let mut expected_max_ts: HashMap<&str, i64> = HashMap::new();
        for row in &rows {
            *expected_counts.entry(&row.session_id).or_default() += 1;
            let slot = expected_max_ts.entry(&row.session_id).or_insert(i64::MIN);
            *slot = (*slot).max(row.timestamp_micros);
            assert_eq!(
                map.lookup(row.row_id),
                Some((row.session_id.as_str(), row.message_id.as_str())),
                "row {} resolves to its own session and message",
                row.row_id,
            );
        }
        for (session, count) in expected_counts {
            assert_eq!(
                map.lookup_count(session),
                Some(count),
                "count for {session}"
            );
            assert_eq!(
                map.lookup_max_ts(session),
                expected_max_ts.get(session).copied(),
                "max timestamp for {session}",
            );
        }
    }

    /// Duplicate `row_id`s inside `appended` are unreachable from a real corpus
    /// (`collect_row_metas_delta` emits each row once), but the merge used to
    /// emit a record per duplicate, which binary search resolves arbitrarily.
    /// They collapse the same way a segment collision does: newest wins.
    #[test]
    fn compaction_collapses_duplicate_appended_row_ids() {
        let dir = tempfile::tempdir().unwrap();
        RowMetaMap::build(
            &RowMetaMap::path_for(dir.path(), "dup", 1),
            1,
            vec![entry(1, "sess-a", "m1", 10, "one")],
        )
        .unwrap();
        let set = RowMetaSet::open(&discover_chain(dir.path(), "dup").expect("chain")).unwrap();

        let compacted = dir.path().join("compacted.rmm");
        set.compact_into(
            &compacted,
            2,
            vec![
                entry(5, "sess-a", "m5-stale", 20, "stale five"),
                entry(5, "sess-a", "m5-newest", 30, "newest five"),
                entry(9, "sess-b", "m9", 40, "nine"),
            ],
        )
        .unwrap();

        let map = RowMetaMap::open(&compacted).unwrap();
        assert_eq!(map.len(), 3, "the duplicate collapses to one record");
        assert_eq!(map.lookup(5), Some(("sess-a", "m5-newest")));
        assert_eq!(
            map.lookup_count("sess-a"),
            Some(2),
            "one row 1 and one row 5"
        );
    }

    /// A segment whose records are not `row_id`-sorted is corrupt, and the merge
    /// cannot absorb it. The pre-streaming compaction sorted its inputs, so such
    /// a chain healed on the next compaction; erroring instead would fail every
    /// later `ensure_rowmap` on that chain forever.
    #[test]
    fn compaction_self_heals_an_unordered_segment() {
        let dir = tempfile::tempdir().unwrap();
        let base = RowMetaMap::path_for(dir.path(), "heal", 1);
        let rows: Vec<RowMetaEntry> = (1..=5u64)
            .map(|i| entry(i, "sess-a", &format!("m{i}"), i as i64, &format!("row {i}")))
            .collect();
        RowMetaMap::build(&base, 1, rows).unwrap();

        // Swap two records in the spine: both still point at valid blob offsets
        // inside the one text block, so every row still reads - the file is
        // simply no longer sorted, which is what breaks `locate` and the merge.
        let mut bytes = std::fs::read(&base).unwrap();
        let spine = size_of::<Header>();
        let record = size_of::<Record>();
        let (first, second) = (spine + record, spine + 3 * record);
        for byte in 0..record {
            bytes.swap(first + byte, second + byte);
        }
        std::fs::write(&base, &bytes).unwrap();

        let set = RowMetaSet::open(&discover_chain(dir.path(), "heal").expect("chain")).unwrap();
        let appended = vec![entry(9, "sess-b", "m9", 9, "nine")];
        assert!(
            set.merge_into(&dir.path().join("merged.rmm"), 2, &appended)
                .expect_err("the swapped spine must break the streaming merge")
                .downcast_ref::<UnorderedRows>()
                .is_some(),
            "the heal below only means anything if the merge really fails first",
        );

        let compacted = dir.path().join("compacted.rmm");
        set.compact_into(&compacted, 2, appended).unwrap();

        let map = RowMetaMap::open(&compacted).unwrap();
        assert_eq!(map.len(), 6);
        for i in 1..=5u64 {
            let message_id = format!("m{i}");
            assert_eq!(
                map.lookup(i),
                Some(("sess-a", message_id.as_str())),
                "row {i} survived the heal",
            );
        }
        assert_eq!(map.lookup(9), Some(("sess-b", "m9")));
    }

    #[test]
    fn out_of_order_rows_are_rejected_and_leave_no_temps() {
        let dir = tempfile::tempdir().unwrap();
        let path = RowMetaMap::path_for(dir.path(), "ooo", 1);
        let rows = [
            entry(10, "sess-a", "m10", 1, "ten"),
            entry(9, "sess-a", "m9", 2, "nine"),
        ];
        let mut builder = RowMetaBuilder::new(&path, 1, rows.len()).unwrap();
        builder.push(rows[0].as_row()).unwrap();
        let error = builder
            .push(rows[1].as_row())
            .expect_err("row 9 after row 10");
        assert!(
            error.downcast_ref::<UnorderedRows>().is_some(),
            "callers key their scan fallback off this type: {error}"
        );
        drop(builder);

        assert!(!path.exists(), "an abandoned build publishes nothing");
        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
            .collect();
        assert!(
            leftovers.is_empty(),
            "an abandoned builder must reclaim its staging temps: {leftovers:?}"
        );
    }

    #[test]
    fn empty_map_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let path = RowMetaMap::path_for(dir.path(), "empty", 1);
        RowMetaMap::build(&path, 1, Vec::new()).unwrap();
        let map = RowMetaMap::open(&path).unwrap();
        assert!(map.is_empty());
        assert_eq!(map.lookup(0), None);
        assert!(map.lookup_meta(0, &mut None).is_none());
        assert_eq!(map.lookup_count("anything"), None);
        assert_eq!(map.lookup_max_ts("anything"), None);
    }
}
