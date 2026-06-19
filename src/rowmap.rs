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
//! Built via temp + atomic rename and `mmap`'d read-only, so N pond processes on
//! the box share one physical copy in the OS page cache and a restart re-`open`s
//! instantly. Stable row ids (`enable_stable_row_ids`) keep a built map valid
//! across compaction; it only rebuilds when the dataset version advances.
//!
//! Layout: `Header | [Record; count] | [SessionEntry] | [DictEntry; project] |
//! [DictEntry; agent] | [DictEntry; role] | [BlockEntry] | blob`. Records are
//! sorted by `row_id` (binary search); a row's block is `record_index /
//! BLOCK_ROWS`. The blob holds the compressed `search_text` blocks, then per-row
//! `{32-byte header + message_id}`, then the dict value bytes.

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::mem::size_of;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use bytemuck::{Pod, Zeroable};
use memmap2::Mmap;

const MAGIC: [u8; 8] = *b"PONDRMM4";
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

    pub fn build(path: &Path, version: u64, mut entries: Vec<RowMetaEntry>) -> Result<()> {
        entries.sort_unstable_by_key(|entry| entry.row_id);

        let mut session_counts: HashMap<&str, u32> = HashMap::new();
        for entry in &entries {
            *session_counts.entry(entry.session_id.as_str()).or_default() += 1;
        }
        let mut sessions: Vec<(&str, u32)> = session_counts.into_iter().collect();
        sessions.sort_unstable_by(|left, right| left.0.cmp(right.0));
        let session_index = index_of(sessions.iter().map(|(value, _)| *value));

        let projects = distinct_sorted(entries.iter().map(|entry| entry.project.as_str()));
        let project_index = index_of(projects.iter().copied());
        let agents = distinct_sorted(entries.iter().map(|entry| entry.source_agent.as_str()));
        let agent_index = index_of(agents.iter().copied());
        let roles = distinct_sorted(entries.iter().map(|entry| entry.role.as_str()));
        let role_index = index_of(roles.iter().copied());

        let mut blob: Vec<u8> = Vec::new();

        // Compressed search_text blocks first; spans record each row's
        // offset+length within its decompressed block.
        let mut block_entries = Vec::with_capacity(entries.len().div_ceil(BLOCK_ROWS));
        let mut spans: Vec<(u32, u32)> = Vec::with_capacity(entries.len());
        for chunk in entries.chunks(BLOCK_ROWS) {
            let mut plain = Vec::new();
            for entry in chunk {
                let off = u32::try_from(plain.len()).context("block too large")?;
                let len = u32::try_from(entry.search_text.len()).context("search_text too long")?;
                plain.extend_from_slice(entry.search_text.as_bytes());
                spans.push((off, len));
            }
            let compressed = zstd::bulk::compress(&plain, ZSTD_LEVEL).context("zstd compress")?;
            block_entries.push(BlockEntry {
                comp_off: blob.len() as u64,
                comp_len: u32::try_from(compressed.len()).context("compressed block too large")?,
                decomp_len: u32::try_from(plain.len()).context("block too large")?,
            });
            blob.extend_from_slice(&compressed);
        }

        let mut records = Vec::with_capacity(entries.len());
        for (entry, (text_off, text_len)) in entries.iter().zip(&spans) {
            let blob_off = blob.len() as u64;
            blob.extend_from_slice(&entry.timestamp_micros.to_le_bytes());
            blob.extend_from_slice(&session_index[entry.session_id.as_str()].to_le_bytes());
            blob.extend_from_slice(&project_index[entry.project.as_str()].to_le_bytes());
            blob.extend_from_slice(&agent_index[entry.source_agent.as_str()].to_le_bytes());
            blob.extend_from_slice(&role_index[entry.role.as_str()].to_le_bytes());
            let mid_len = u32::try_from(entry.message_id.len()).context("message_id too long")?;
            blob.extend_from_slice(&mid_len.to_le_bytes());
            blob.extend_from_slice(&text_off.to_le_bytes());
            blob.extend_from_slice(&text_len.to_le_bytes());
            blob.extend_from_slice(entry.message_id.as_bytes());
            records.push(Record {
                row_id: entry.row_id,
                blob_off,
            });
        }

        let session_entries = sessions
            .iter()
            .map(|(sid, count)| {
                let off = blob.len() as u64;
                blob.extend_from_slice(sid.as_bytes());
                Ok(SessionEntry {
                    sid_off: off,
                    sid_len: u32::try_from(sid.len()).context("session_id too long")?,
                    count: *count,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let project_entries = dict_entries(&mut blob, &projects)?;
        let agent_entries = dict_entries(&mut blob, &agents)?;
        let role_entries = dict_entries(&mut blob, &roles)?;

        let blob_offset = (size_of::<Header>()
            + records.len() * size_of::<Record>()
            + session_entries.len() * size_of::<SessionEntry>()
            + (project_entries.len() + agent_entries.len() + role_entries.len())
                * size_of::<DictEntry>()
            + block_entries.len() * size_of::<BlockEntry>()) as u64;
        let header = Header {
            magic: MAGIC,
            version,
            count: records.len() as u64,
            session_count: session_entries.len() as u64,
            project_count: project_entries.len() as u64,
            agent_count: agent_entries.len() as u64,
            role_count: role_entries.len() as u64,
            block_count: block_entries.len() as u64,
            blob_offset,
        };

        // Unique temp name per builder (pid + nonce): two processes prewarming
        // the same store+version must not share one temp inode, or the second's
        // create would mutate the file the first is mapping.
        let tmp = path.with_extension(format!(
            "tmp-{}-{:016x}",
            std::process::id(),
            fastrand::u64(..)
        ));
        {
            let mut file = File::create(&tmp)
                .with_context(|| format!("create row meta map temp {}", tmp.display()))?;
            file.write_all(bytemuck::bytes_of(&header))?;
            file.write_all(bytemuck::cast_slice(&records))?;
            file.write_all(bytemuck::cast_slice(&session_entries))?;
            file.write_all(bytemuck::cast_slice(&project_entries))?;
            file.write_all(bytemuck::cast_slice(&agent_entries))?;
            file.write_all(bytemuck::cast_slice(&role_entries))?;
            file.write_all(bytemuck::cast_slice(&block_entries))?;
            file.write_all(&blob)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename row meta map into place {}", path.display()))?;
        Ok(())
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

    /// Reconstruct every row as an owned [`RowMetaEntry`], decompressing each
    /// block once. Used to merge segments into a fresh base at compaction
    /// without re-reading the store.
    pub fn entries(&self) -> Vec<RowMetaEntry> {
        let records = self.records();
        let mut out = Vec::with_capacity(records.len());
        let mut current_block = usize::MAX;
        let mut plain: Vec<u8> = Vec::new();
        for (idx, record) in records.iter().enumerate() {
            let block_idx = idx / BLOCK_ROWS;
            if block_idx != current_block {
                plain = self.decompress_block(block_idx).unwrap_or_default();
                current_block = block_idx;
            }
            let Some(base) = self.blob_offset.checked_add(record.blob_off as usize) else {
                continue;
            };
            if let Some(entry) = self.entry_at(record.row_id, base, &plain) {
                out.push(entry);
            }
        }
        out
    }

    fn entry_at(&self, row_id: u64, base: usize, block_plain: &[u8]) -> Option<RowMetaEntry> {
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
        let message_id = self.slice_str(&mut at, mid_len)?.to_owned();
        let search_text = if text_len == 0 {
            String::new()
        } else {
            let bytes = block_plain.get(text_off..text_off.checked_add(text_len)?)?;
            String::from_utf8(bytes.to_vec()).ok()?
        };
        Some(RowMetaEntry {
            row_id,
            session_id: self.session_str(session_idx).to_owned(),
            message_id,
            role: self
                .dict_str(self.roles_off, self.role_count, role_idx)
                .to_owned(),
            project: self
                .dict_str(self.projects_off, self.project_count, project_idx)
                .to_owned(),
            source_agent: self
                .dict_str(self.agents_off, self.agent_count, agent_idx)
                .to_owned(),
            timestamp_micros,
            search_text,
        })
    }

    /// Whole-session message count for `session_id`. `None` if the session is
    /// not in this map (caller falls back to the `session_id IN (...)` scan).
    pub fn lookup_count(&self, session_id: &str) -> Option<usize> {
        let entries = self.session_entries();
        let idx = entries
            .binary_search_by(|entry| self.blob_str(entry.sid_off, entry.sid_len).cmp(session_id))
            .ok()?;
        Some(entries[idx].count as usize)
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

    /// Every row across all segments, newest-segment-wins on `row_id`
    /// collision - the input to a base rebuild at compaction.
    pub fn merged_entries(&self) -> Vec<RowMetaEntry> {
        let mut by_row: HashMap<u64, RowMetaEntry> = HashMap::new();
        for seg in &self.segments {
            for entry in seg.entries() {
                by_row.insert(entry.row_id, entry);
            }
        }
        by_row.into_values().collect()
    }
}

fn distinct_sorted<'a>(values: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
    let mut distinct: Vec<&str> = values.collect();
    distinct.sort_unstable();
    distinct.dedup();
    distinct
}

fn index_of<'a>(values: impl Iterator<Item = &'a str>) -> HashMap<&'a str, u32> {
    values
        .enumerate()
        .map(|(index, value)| (value, index as u32))
        .collect()
}

fn dict_entries(blob: &mut Vec<u8>, values: &[&str]) -> Result<Vec<DictEntry>> {
    values
        .iter()
        .map(|value| {
            let off = blob.len() as u64;
            blob.extend_from_slice(value.as_bytes());
            Ok(DictEntry {
                off,
                len: u32::try_from(value.len()).context("dictionary value too long")?,
                _pad: 0,
            })
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

        // Compaction input: all 5 distinct rows reconstructed.
        let mut merged = set.merged_entries();
        merged.sort_by_key(|entry| entry.row_id);
        assert_eq!(merged.len(), 5);
        assert_eq!(merged[0].row_id, 10);
        assert_eq!(merged[4].row_id, 21);
        assert_eq!(merged[4].search_text, "delta twentyone");
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
    }
}
