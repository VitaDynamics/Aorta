//! Test-only MCAP fixture builder and byte-level surgery (MCAP Player Proposal
//! §7).
//!
//! Fixtures are built with the `mcap` crate's own `Writer` — the recorder's
//! helpers are private (§7) — and then mutated at the
//! byte level to produce the crash/corruption shapes the §7 item 3 matrix
//! demands: truncation at arbitrary offsets, stripped footers, flipped bytes
//! inside a framed chunk, zeroed CRC fields, and content engineered so a CRC32
//! *genuinely calculates* to `0x00000000` (the §6.1 "stored zero + calculated
//! zero" case).

use mcap::records::{op, MessageHeader, Metadata};
use mcap::{Compression, WriteOptions};
use std::collections::BTreeMap;
use std::io::Cursor;

/// `opcode` (1) + `length` (8) — the outer record envelope.
pub(crate) const OPCODE_LEN_SIZE: usize = 9;
/// `message_start_time` + `message_end_time` + `uncompressed_size` +
/// `uncompressed_crc` + compression-name length + `compressed_size`.
pub(crate) const MIN_CHUNK_HEADER_SIZE: usize = 8 + 8 + 8 + 4 + 4 + 8;
/// Private-record opcode used for CRC-forcing padding (MCAP reserves
/// `0x80..=0xFF` for application extensions).
pub(crate) const PADDING_OPCODE: u8 = 0x80;

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct FixtureOptions {
    pub compression: Option<Compression>,
    pub use_chunks: bool,
    pub chunk_size: Option<u64>,
    pub summary: bool,
    pub chunk_crcs: bool,
    pub data_section_crc: bool,
}

impl Default for FixtureOptions {
    fn default() -> Self {
        Self {
            compression: None,
            use_chunks: true,
            // No size-driven chunk splitting: fixtures decide boundaries with
            // an explicit flush so the record stream is deterministic.
            chunk_size: None,
            summary: true,
            chunk_crcs: true,
            data_section_crc: true,
        }
    }
}

impl FixtureOptions {
    pub(crate) fn unchunked() -> Self {
        Self {
            use_chunks: false,
            ..Self::default()
        }
    }

    pub(crate) fn compressed(compression: Compression) -> Self {
        Self {
            compression: Some(compression),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaSpec {
    pub name: String,
    pub encoding: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct ChannelSpec {
    pub topic: String,
    pub message_encoding: String,
    pub schema: Option<SchemaSpec>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub(crate) struct MessageSpec {
    pub channel: usize,
    pub log_time: u64,
    pub data: Vec<u8>,
}

/// Builds MCAP bytes through the `mcap` writer.
#[derive(Debug, Clone)]
pub(crate) struct FixtureBuilder {
    options: FixtureOptions,
    channels: Vec<ChannelSpec>,
    messages: Vec<MessageSpec>,
    metadata: Vec<(String, BTreeMap<String, String>)>,
    flush_after: Vec<usize>,
}

impl FixtureBuilder {
    pub(crate) fn new(options: FixtureOptions) -> Self {
        Self {
            options,
            channels: Vec::new(),
            messages: Vec::new(),
            metadata: Vec::new(),
            flush_after: Vec::new(),
        }
    }

    /// A typed `pub/**` channel carrying a bfbs schema, as the recorder writes.
    pub(crate) fn typed_channel(self, topic: &str, schema_name: &str, bfbs: &[u8]) -> Self {
        self.channel(typed_channel_spec(topic, schema_name, bfbs))
    }

    /// A schema-less (`aorta_raw_bytes`) channel.
    pub(crate) fn raw_channel(self, topic: &str) -> Self {
        self.channel(raw_channel_spec(topic))
    }

    /// Push a fully-specified channel — the escape hatch the MCAP Player
    /// Proposal §6.1/§6.2 validation tests use to write inconsistent metadata
    /// on purpose.
    pub(crate) fn channel(mut self, spec: ChannelSpec) -> Self {
        self.channels.push(spec);
        self
    }

    /// The `aorta.recorder.start` metadata record, exactly as `writer.rs`
    /// writes it: `aorta.group` + `aorta.started_at_ns` always, segment
    /// identity only on the rotating path.
    pub(crate) fn recorder_start(
        self,
        group: &str,
        started_at_ns: u64,
        segment: Option<(&str, u64, bool)>,
    ) -> Self {
        let started = started_at_ns.to_string();
        let mut entries: Vec<(&str, &str)> = vec![
            ("aorta.profile", "aorta"),
            ("aorta.group", group),
            ("aorta.started_at_ns", started.as_str()),
        ];
        let (index, continues);
        if let Some((recording_id, segment_index, segment_continues)) = segment {
            index = segment_index.to_string();
            continues = segment_continues.to_string();
            entries.push(("aorta.recording_id", recording_id));
            entries.push(("aorta.segment.index", index.as_str()));
            entries.push(("aorta.segment.continues", continues.as_str()));
        }
        self.metadata("aorta.recorder.start", &entries)
    }

    pub(crate) fn message(mut self, channel: usize, log_time: u64, data: &[u8]) -> Self {
        self.messages.push(MessageSpec {
            channel,
            log_time,
            data: data.to_vec(),
        });
        self
    }

    pub(crate) fn metadata(mut self, name: &str, entries: &[(&str, &str)]) -> Self {
        let map = entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.metadata.push((name.to_string(), map));
        self
    }

    /// Force a chunk boundary after the message at `index`.
    pub(crate) fn flush_after(mut self, index: usize) -> Self {
        self.flush_after.push(index);
        self
    }

    pub(crate) fn build(self) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = WriteOptions::new()
                .profile("aorta")
                .library("aorta-play-fixture")
                .compression(self.options.compression)
                .use_chunks(self.options.use_chunks)
                .chunk_size(self.options.chunk_size)
                .emit_summary_records(self.options.summary)
                .calculate_chunk_crcs(self.options.chunk_crcs)
                .calculate_data_section_crc(self.options.data_section_crc)
                .create(&mut cursor)
                .expect("create fixture writer");

            for (name, entries) in &self.metadata {
                writer
                    .write_metadata(&Metadata {
                        name: name.clone(),
                        metadata: entries.clone(),
                    })
                    .expect("write metadata");
            }

            let mut channel_ids = Vec::with_capacity(self.channels.len());
            for channel in &self.channels {
                let schema_id = match &channel.schema {
                    Some(schema) => writer
                        .add_schema(&schema.name, &schema.encoding, &schema.data)
                        .expect("add schema"),
                    None => 0,
                };
                channel_ids.push(
                    writer
                        .add_channel(
                            schema_id,
                            &channel.topic,
                            &channel.message_encoding,
                            &channel.metadata,
                        )
                        .expect("add channel"),
                );
            }

            for (index, message) in self.messages.iter().enumerate() {
                writer
                    .write_to_known_channel(
                        &MessageHeader {
                            channel_id: channel_ids[message.channel],
                            sequence: index as u32,
                            log_time: message.log_time,
                            publish_time: message.log_time,
                        },
                        &message.data,
                    )
                    .expect("write message");
                if self.flush_after.contains(&index) {
                    writer.flush().expect("flush chunk");
                }
            }

            writer.finish().expect("finish fixture");
        }
        cursor.into_inner()
    }
}

/// The group-relative suffix of an `aorta/{group}/{suffix}` key.
pub(crate) fn key_suffix_of(topic: &str) -> String {
    topic.splitn(3, '/').nth(2).unwrap_or(topic).to_string()
}

/// The canonical schema hash the recorder stores in channel metadata.
pub(crate) fn schema_hash_hex(bfbs: &[u8]) -> String {
    aorta_core::schema::SchemaHash::from_bfbs_bytes(bfbs).to_hex()
}

/// A typed channel with the exact metadata triple `writer.rs` writes.
pub(crate) fn typed_channel_spec(topic: &str, schema_name: &str, bfbs: &[u8]) -> ChannelSpec {
    ChannelSpec {
        topic: topic.to_string(),
        message_encoding: "flatbuffer".to_string(),
        schema: Some(SchemaSpec {
            name: schema_name.to_string(),
            encoding: "flatbuffer".to_string(),
            data: bfbs.to_vec(),
        }),
        metadata: channel_metadata(topic, &schema_hash_hex(bfbs)),
    }
}

/// A schema-less channel; the recorder stores the literal `raw` hash marker.
pub(crate) fn raw_channel_spec(topic: &str) -> ChannelSpec {
    ChannelSpec {
        topic: topic.to_string(),
        message_encoding: "aorta_raw_bytes".to_string(),
        schema: None,
        metadata: channel_metadata(topic, "raw"),
    }
}

pub(crate) fn channel_metadata(topic: &str, schema_hash: &str) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    metadata.insert("aorta_key".to_string(), topic.to_string());
    metadata.insert("key_suffix".to_string(), key_suffix_of(topic));
    metadata.insert("schema_hash".to_string(), schema_hash.to_string());
    metadata
}

/// One typed channel, three messages, an `aorta.recorder.start` metadata
/// record — the smallest shape that exercises every yielded record kind.
pub(crate) fn simple_file(options: FixtureOptions) -> Vec<u8> {
    FixtureBuilder::new(options)
        .recorder_start("robot", 1000, None)
        .typed_channel(
            "aorta/robot/pub/sensor/imu",
            "aorta.sensor.Imu",
            b"bfbs-imu",
        )
        .message(0, 100, b"payload-0")
        .message(0, 200, b"payload-1")
        .message(0, 300, b"payload-2")
        .build()
}

// ---------------------------------------------------------------------------
// Byte-level inspection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Span {
    pub offset: usize,
    pub opcode: u8,
    pub len: u64,
}

impl Span {
    pub(crate) fn end(&self) -> usize {
        self.offset + OPCODE_LEN_SIZE + self.len as usize
    }

    pub(crate) fn body(&self) -> usize {
        self.offset + OPCODE_LEN_SIZE
    }
}

/// Walk the outer record envelopes, independently of the production framer.
pub(crate) fn top_level_records(bytes: &[u8]) -> Vec<Span> {
    let mut spans = Vec::new();
    let mut offset = mcap::MAGIC.len();
    while offset + OPCODE_LEN_SIZE <= bytes.len() {
        let opcode = bytes[offset];
        let len = u64::from_le_bytes(
            bytes[offset + 1..offset + OPCODE_LEN_SIZE]
                .try_into()
                .expect("9-byte envelope"),
        );
        let span = Span {
            offset,
            opcode,
            len,
        };
        spans.push(span);
        if span.end() > bytes.len() {
            break;
        }
        offset = span.end();
        if opcode == op::FOOTER {
            break;
        }
    }
    spans
}

pub(crate) fn find_record(bytes: &[u8], opcode: u8) -> Span {
    top_level_records(bytes)
        .into_iter()
        .find(|span| span.opcode == opcode)
        .unwrap_or_else(|| panic!("no record with opcode {opcode:#04x} in fixture"))
}

/// Field offsets of a chunk record, resolved from its header.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChunkFields {
    pub record: Span,
    pub uncompressed_size: u64,
    pub uncompressed_size_offset: usize,
    pub crc: u32,
    pub crc_offset: usize,
    pub compression_len: u32,
    pub compression_len_offset: usize,
    pub compressed_size: u64,
    pub compressed_size_offset: usize,
    pub records_offset: usize,
}

pub(crate) fn chunk_fields(bytes: &[u8], span: Span) -> ChunkFields {
    assert_eq!(span.opcode, op::CHUNK, "not a chunk record");
    assert!(
        span.len >= MIN_CHUNK_HEADER_SIZE as u64,
        "chunk record is shorter than its header"
    );
    let body = span.body();
    let compression_len = read_u32(bytes, body + 28);
    let compressed_size_offset = body + 32 + compression_len as usize;
    ChunkFields {
        record: span,
        uncompressed_size: read_u64(bytes, body + 16),
        uncompressed_size_offset: body + 16,
        crc: read_u32(bytes, body + 24),
        crc_offset: body + 24,
        compression_len,
        compression_len_offset: body + 28,
        compressed_size: read_u64(bytes, compressed_size_offset),
        compressed_size_offset,
        records_offset: compressed_size_offset + 8,
    }
}

pub(crate) fn first_chunk(bytes: &[u8]) -> ChunkFields {
    chunk_fields(bytes, find_record(bytes, op::CHUNK))
}

pub(crate) fn read_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("u32"))
}

pub(crate) fn read_u64(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("u64"))
}

pub(crate) fn put_u32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u64(bytes: &mut [u8], at: usize, value: u64) {
    bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Byte-level surgery
// ---------------------------------------------------------------------------

pub(crate) fn truncate_at(bytes: &[u8], at: usize) -> Vec<u8> {
    bytes[..at].to_vec()
}

/// Drop the trailing end magic (the crashed-recorder signature, MCAP Player
/// Proposal §2).
pub(crate) fn strip_end_magic(bytes: &[u8]) -> Vec<u8> {
    truncate_at(bytes, bytes.len() - mcap::MAGIC.len())
}

/// Drop the footer record *and* the end magic.
pub(crate) fn strip_footer(bytes: &[u8]) -> Vec<u8> {
    let footer = find_record(bytes, op::FOOTER);
    truncate_at(bytes, footer.offset)
}

pub(crate) fn flip_byte(bytes: &mut [u8], at: usize) {
    bytes[at] ^= 0xFF;
}

pub(crate) fn splice(bytes: &[u8], at: usize, insert: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + insert.len());
    out.extend_from_slice(&bytes[..at]);
    out.extend_from_slice(insert);
    out.extend_from_slice(&bytes[at..]);
    out
}

pub(crate) fn remove_span(bytes: &[u8], span: Span) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() - (span.end() - span.offset));
    out.extend_from_slice(&bytes[..span.offset]);
    out.extend_from_slice(&bytes[span.end()..]);
    out
}

/// A 13-byte private record whose last four bytes are the CRC-forcing suffix.
pub(crate) fn padding_record(suffix: [u8; 4]) -> Vec<u8> {
    let mut record = Vec::with_capacity(OPCODE_LEN_SIZE + 4);
    record.push(PADDING_OPCODE);
    record.extend_from_slice(&4u64.to_le_bytes());
    record.extend_from_slice(&suffix);
    record
}

/// The prefix of [`padding_record`] before the tunable suffix.
pub(crate) fn padding_record_prefix() -> Vec<u8> {
    let mut prefix = Vec::with_capacity(OPCODE_LEN_SIZE);
    prefix.push(PADDING_OPCODE);
    prefix.extend_from_slice(&4u64.to_le_bytes());
    prefix
}

/// Recompute the stored data-section CRC over `[0, DataEnd)` (MCAP Player
/// Proposal §2). Any splice into the data section invalidates it, so surgery
/// helpers call this last unless the test wants the mismatch.
pub(crate) fn fix_data_section_crc(bytes: &mut [u8]) {
    let data_end = find_record(bytes, op::DATA_END);
    let crc = crc32(&bytes[..data_end.offset]);
    put_u32(bytes, data_end.body(), crc);
}

pub(crate) fn set_data_section_crc(bytes: &mut [u8], value: u32) {
    let data_end = find_record(bytes, op::DATA_END);
    put_u32(bytes, data_end.body(), value);
}

pub(crate) fn data_section_crc(bytes: &[u8]) -> u32 {
    let data_end = find_record(bytes, op::DATA_END);
    crc32(&bytes[..data_end.offset])
}

/// Splice a padding record into the *uncompressed* records field of the first
/// chunk and tune it so the chunk's CRC32 calculates to exactly zero, then
/// store zero as well: the MCAP Player Proposal §6.1 "stored zero + calculated
/// zero" case.
pub(crate) fn make_chunk_crc_calculate_zero(bytes: &[u8]) -> Vec<u8> {
    make_chunk_crc_calculate(bytes, 0)
}

/// Engineer the first uncompressed chunk so its true CRC32 calculates to
/// `target` while the stored chunk CRC says zero.
pub(crate) fn make_chunk_crc_calculate(bytes: &[u8], target: u32) -> Vec<u8> {
    let chunk = first_chunk(bytes);
    assert_eq!(
        chunk.compression_len, 0,
        "CRC forcing needs an uncompressed chunk"
    );
    let records_end = chunk.records_offset + chunk.compressed_size as usize;

    let mut forced = bytes[chunk.records_offset..records_end].to_vec();
    forced.extend_from_slice(&padding_record_prefix());
    let suffix = force_crc32_suffix(&forced, target);
    forced.extend_from_slice(&suffix);
    assert_eq!(crc32(&forced), target, "CRC forcing failed");

    let padding = padding_record(suffix);
    let mut out = splice(bytes, records_end, &padding);
    let grown = padding.len() as u64;
    put_u64(
        &mut out,
        chunk.record.offset + 1,
        chunk.record.len + grown, // outer record length
    );
    put_u64(
        &mut out,
        chunk.uncompressed_size_offset,
        chunk.uncompressed_size + grown,
    );
    put_u64(
        &mut out,
        chunk.compressed_size_offset,
        chunk.compressed_size + grown,
    );
    put_u32(&mut out, chunk.crc_offset, 0);
    fix_data_section_crc(&mut out);
    out
}

/// Splice a padding record immediately before `DataEnd` and tune it so the
/// data section's CRC32 calculates to exactly zero, then store zero as well.
pub(crate) fn make_data_section_crc_calculate_zero(bytes: &[u8]) -> Vec<u8> {
    make_data_section_crc_calculate(bytes, 0)
}

/// Engineer the data section so its true CRC32 calculates to `target` while
/// the stored `DataEnd` field says zero — the raw material for both the
/// accepted stored-0/calculated-0 case and the MCAP Player Proposal §6.7 item 5
/// probe-sentinel collision case.
pub(crate) fn make_data_section_crc_calculate(bytes: &[u8], target: u32) -> Vec<u8> {
    let data_end = find_record(bytes, op::DATA_END);
    let mut prefix = bytes[..data_end.offset].to_vec();
    prefix.extend_from_slice(&padding_record_prefix());
    let suffix = force_crc32_suffix(&prefix, target);
    prefix.extend_from_slice(&suffix);
    assert_eq!(crc32(&prefix), target, "CRC forcing failed");

    let mut out = splice(bytes, data_end.offset, &padding_record(suffix));
    set_data_section_crc(&mut out, 0);
    out
}

// ---------------------------------------------------------------------------
// CRC-32 (ISO-HDLC / zlib) and suffix forcing
// ---------------------------------------------------------------------------

fn crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut value = i as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 != 0 {
                0xEDB8_8320 ^ (value >> 1)
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[i] = value;
        i += 1;
    }
    table
}

pub(crate) fn crc32(data: &[u8]) -> u32 {
    let table = crc32_table();
    let mut register = 0xFFFF_FFFFu32;
    for byte in data {
        register = (register >> 8) ^ table[((register ^ u32::from(*byte)) & 0xFF) as usize];
    }
    !register
}

/// Return the four bytes that, appended to `prefix`, make
/// `crc32(prefix || suffix) == target`.
///
/// CRC32 is affine, so the last four bytes can always be solved for: step the
/// register backwards from the target four times (each table entry has a unique
/// high byte, which identifies the table index used by that step), then replay
/// forwards from the prefix's register to recover the byte values.
pub(crate) fn force_crc32_suffix(prefix: &[u8], target: u32) -> [u8; 4] {
    let table = crc32_table();
    let mut indices = [0usize; 4];
    let mut register = target ^ 0xFFFF_FFFF;
    for slot in indices.iter_mut().rev() {
        let index = (0..256)
            .find(|&i| table[i] >> 24 == register >> 24)
            .expect("CRC32 table high bytes are a permutation");
        *slot = index;
        register = (register ^ table[index]) << 8;
    }

    let mut suffix = [0u8; 4];
    let mut register = crc32(prefix) ^ 0xFFFF_FFFF;
    for (byte, index) in suffix.iter_mut().zip(indices) {
        *byte = (register & 0xFF) as u8 ^ index as u8;
        register = (register >> 8) ^ table[index];
    }
    assert_eq!(register ^ 0xFFFF_FFFF, target, "CRC32 forcing is exact");
    suffix
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_the_reference_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn crc32_table_high_bytes_are_a_permutation() {
        let table = crc32_table();
        let mut seen = [false; 256];
        for entry in table {
            let high = (entry >> 24) as usize;
            assert!(!seen[high], "duplicate high byte {high}");
            seen[high] = true;
        }
    }

    #[test]
    fn force_crc32_suffix_hits_zero_and_arbitrary_targets() {
        for target in [0u32, 1, 0xDEAD_BEEF, 0xFFFF_FFFF] {
            for prefix in [b"".as_slice(), b"aorta".as_slice(), &[0u8; 257]] {
                let suffix = force_crc32_suffix(prefix, target);
                let mut full = prefix.to_vec();
                full.extend_from_slice(&suffix);
                assert_eq!(crc32(&full), target);
            }
        }
    }

    #[test]
    fn builder_produces_a_walkable_chunked_file() {
        let bytes = simple_file(FixtureOptions::default());
        assert_eq!(&bytes[..mcap::MAGIC.len()], mcap::MAGIC);
        assert_eq!(&bytes[bytes.len() - mcap::MAGIC.len()..], mcap::MAGIC);
        let spans = top_level_records(&bytes);
        assert!(spans.iter().any(|s| s.opcode == op::CHUNK));
        assert!(spans.iter().any(|s| s.opcode == op::DATA_END));
        assert!(spans.iter().any(|s| s.opcode == op::FOOTER));
        assert_eq!(data_section_crc(&bytes), {
            let data_end = find_record(&bytes, op::DATA_END);
            read_u32(&bytes, data_end.body())
        });
    }

    #[test]
    fn builder_can_disable_chunking_summaries_and_crcs() {
        let bytes = simple_file(FixtureOptions {
            use_chunks: false,
            summary: false,
            chunk_crcs: false,
            data_section_crc: false,
            ..FixtureOptions::default()
        });
        let spans = top_level_records(&bytes);
        assert!(spans.iter().all(|s| s.opcode != op::CHUNK));
        assert!(spans.iter().filter(|s| s.opcode == op::MESSAGE).count() == 3);
        let data_end = find_record(&bytes, op::DATA_END);
        assert_eq!(read_u32(&bytes, data_end.body()), 0, "stored zero sentinel");
    }
}
