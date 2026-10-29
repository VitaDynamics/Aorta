use std::collections::HashSet;
use std::fmt;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use flatbuffers_reflection::reflection;
use sha2::{Digest, Sha256};

use crate::error::{AortaError, AortaResult};
use crate::runtime_env;

const SCHEMA_HASH_HEX_LEN: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaHash([u8; 32]);

impl SchemaHash {
    pub fn from_bfbs_bytes(bytes: &[u8]) -> Self {
        let digest = Sha256::digest(bytes);
        let mut raw = [0u8; 32];
        raw.copy_from_slice(&digest);
        SchemaHash(raw)
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        SchemaHash(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex_bytes(self) -> [u8; SCHEMA_HASH_HEX_LEN] {
        let mut out = [0u8; SCHEMA_HASH_HEX_LEN];
        for (idx, byte) in self.0.iter().enumerate() {
            let hi = byte >> 4;
            let lo = byte & 0x0f;
            out[idx * 2] = nibble_to_hex(hi);
            out[idx * 2 + 1] = nibble_to_hex(lo);
        }
        out
    }

    pub fn to_hex(self) -> String {
        let bytes = self.to_hex_bytes();
        String::from_utf8(bytes.to_vec()).expect("hex is valid utf8")
    }

    pub fn from_hex_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != SCHEMA_HASH_HEX_LEN {
            return None;
        }
        let mut raw = [0u8; 32];
        for (idx, chunk) in bytes.chunks(2).enumerate() {
            let hi = hex_to_nibble(chunk[0])?;
            let lo = hex_to_nibble(chunk[1])?;
            raw[idx] = (hi << 4) | lo;
        }
        Some(SchemaHash(raw))
    }

    pub fn from_hex_str(value: &str) -> Option<Self> {
        Self::from_hex_bytes(value.as_bytes())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaAttachPolicy {
    Inherit,
    Always,
    Never,
}

impl SchemaAttachPolicy {
    pub fn from_env() -> Self {
        match runtime_env::var("AORTA_SCHEMA_ATTACH")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "always" => SchemaAttachPolicy::Always,
            "never" => SchemaAttachPolicy::Never,
            _ => SchemaAttachPolicy::Never,
        }
    }

    pub fn resolve(self, node_default: SchemaAttachPolicy) -> SchemaAttachPolicy {
        match self {
            SchemaAttachPolicy::Inherit => node_default,
            other => other,
        }
    }
}

impl fmt::Debug for SchemaHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaHash(sha256={})", self.to_hex())
    }
}

impl fmt::Display for SchemaHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

fn nibble_to_hex(value: u8) -> u8 {
    match value {
        0..=9 => b'0' + value,
        10..=15 => b'a' + (value - 10),
        _ => b'0',
    }
}

fn hex_to_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

pub trait SchemaMeta {
    fn schema_bfbs() -> &'static [u8];

    /// Fully-qualified FlatBuffers root type name for this schema (for example
    /// `demo.rpc.EchoRequest`).
    ///
    /// Returning `None` keeps backward compatibility for legacy call sites
    /// that do not use schema_by_key root metadata.
    fn root_type() -> Option<&'static str> {
        None
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::from_bfbs_bytes(Self::schema_bfbs())
    }

    /// Returns fixed serialized size in bytes if the message is fixed-size.
    /// Variable-size messages should return None.
    fn fixed_size() -> Option<usize> {
        None
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HeaderValues<'a> {
    pub publish_stamp_ns: u64,
    pub publisher_node: &'a str,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AortaHeaderLayout {
    pub root_aorta_header_field_id: usize,
    pub header_publisher_node_field_id: usize,
    pub header_sequence_field_id: usize,
}

pub trait HeaderInjector {
    type Header<'a>;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        values: &HeaderValues<'_>,
    ) -> WIPOffset<Self::Header<'b>>;
}

pub trait TypedPublishMessage: SchemaMeta + HeaderInjector {
    type Msg<'a>: Default;
    type Root<'a>;

    fn build_typed_root<'a>(
        builder: &mut FlatBufferBuilder<'a>,
        header: WIPOffset<Self::Header<'a>>,
        msg: &Self::Msg<'_>,
    ) -> WIPOffset<Self::Root<'a>>;
}

/// Generic FlatBuffers root decode helper for typed subscriber implementations.
pub fn decode_flatbuffer_root<'a, R>(
    payload: &'a [u8],
) -> AortaResult<<R as flatbuffers::Follow<'a>>::Inner>
where
    R: flatbuffers::Follow<'a> + flatbuffers::Verifiable + 'a,
{
    flatbuffers::root::<R>(payload).map_err(|err| {
        AortaError::invalid_argument(format!("typed subscriber decode failed: {err}"))
    })
}

/// Typed decode contract for subscriber callbacks.
///
/// SDK-level typed subscriber APIs consume this trait.
pub trait TypedSubscribeMessage: SchemaMeta {
    type Root<'a>;

    fn decode_typed_root<'a>(payload: &'a [u8]) -> AortaResult<Self::Root<'a>>;
}

const REQUEST_SUFFIXES: [&str; 2] = ["Request", "Req"];
const RESPONSE_SUFFIXES: [&str; 2] = ["Response", "Resp"];

fn normalize_root_type(value: Option<&str>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn infer_counterpart_root_type(
    current: Option<&str>,
    object_names: &HashSet<String>,
    from_suffixes: &[&str],
    to_suffixes: &[&str],
) -> Option<String> {
    let current = current?;
    for (from_suffix, to_suffix) in from_suffixes.iter().zip(to_suffixes.iter()) {
        if let Some(prefix) = current.strip_suffix(from_suffix) {
            let candidate = format!("{}{}", prefix, to_suffix);
            if object_names.contains(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Returns the BFBS default root table name (for example `demo.Chat`) if available.
pub fn bfbs_root_type_name(bfbs: &[u8]) -> Option<String> {
    if bfbs.is_empty() {
        return None;
    }
    let schema = reflection::root_as_schema(bfbs).ok()?;
    let root = schema.root_table()?;
    let name = root.name().trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Infer payload header field ids from BFBS reflection metadata.
///
/// This resolves:
/// - root table field id for `aorta_header`
/// - nested `AortaHeader` field ids for `publisher_node` and `sequence`
pub fn infer_aorta_header_layout_from_bfbs(bfbs: &[u8]) -> Option<AortaHeaderLayout> {
    if bfbs.is_empty() {
        return None;
    }

    let schema = reflection::root_as_schema(bfbs).ok()?;
    let root = schema.root_table()?;

    let root_aorta_header = root
        .fields()
        .iter()
        .find(|field| field.name() == "aorta_header")?;
    let header_type = root_aorta_header.type_();
    if header_type.base_type() != reflection::BaseType::Obj {
        return None;
    }

    let header_object_index = usize::try_from(header_type.index()).ok()?;
    let objects = schema.objects();
    if header_object_index >= objects.len() {
        return None;
    }
    let header_object = objects.get(header_object_index);
    if header_object.is_struct() {
        return None;
    }

    let header_publisher_node = header_object
        .fields()
        .iter()
        .find(|field| field.name() == "publisher_node")?;
    let header_sequence = header_object
        .fields()
        .iter()
        .find(|field| field.name() == "sequence")?;

    Some(AortaHeaderLayout {
        root_aorta_header_field_id: usize::from(root_aorta_header.id()),
        header_publisher_node_field_id: usize::from(header_publisher_node.id()),
        header_sequence_field_id: usize::from(header_sequence.id()),
    })
}

/// Infer missing service request/response root types from BFBS metadata.
///
/// Inference rules:
/// - Keep caller-provided non-empty root names as-is.
/// - When both roots are missing, use BFBS default root as request root.
/// - Try `Request <-> Response` and `Req <-> Resp` counterpart inference.
/// - If parsing fails, preserve caller-provided values without modification.
pub fn infer_service_root_types_from_bfbs(
    bfbs: &[u8],
    request_root_type: Option<&str>,
    response_root_type: Option<&str>,
) -> (Option<String>, Option<String>) {
    let mut request = normalize_root_type(request_root_type);
    let mut response = normalize_root_type(response_root_type);

    if request.is_some() && response.is_some() {
        return (request, response);
    }

    if bfbs.is_empty() {
        return (request, response);
    }

    let schema = match reflection::root_as_schema(bfbs) {
        Ok(schema) => schema,
        Err(_) => return (request, response),
    };

    let root_name = schema
        .root_table()
        .map(|object| object.name().trim().to_string())
        .filter(|name| !name.is_empty());

    let mut object_names: HashSet<String> = HashSet::new();
    for object in schema.objects().iter() {
        if object.is_struct() {
            continue;
        }
        let name = object.name().trim();
        if !name.is_empty() {
            object_names.insert(name.to_string());
        }
    }

    if request.is_none() && response.is_none() {
        if let Some(root) = root_name.as_ref() {
            request = Some(root.clone());
        }
    }

    if request.is_none() {
        request = infer_counterpart_root_type(
            response.as_deref(),
            &object_names,
            &RESPONSE_SUFFIXES,
            &REQUEST_SUFFIXES,
        );
        if request.is_none() {
            if let Some(root) = root_name.as_ref() {
                if response.as_deref() != Some(root.as_str()) {
                    request = Some(root.clone());
                }
            }
        }
    }

    if response.is_none() {
        response = infer_counterpart_root_type(
            request.as_deref(),
            &object_names,
            &REQUEST_SUFFIXES,
            &RESPONSE_SUFFIXES,
        );
        if response.is_none() {
            if let Some(root) = root_name.as_ref() {
                if request.as_deref() != Some(root.as_str()) && root.ends_with("Response") {
                    response = Some(root.clone());
                }
            }
        }
    }

    (request, response)
}

pub fn bfbs_contains_aorta_header(bfbs: &[u8]) -> bool {
    // Use the null-terminated field name so we match exact FlatBuffers field
    // identifiers and avoid false positives from substrings like
    // "my_aorta_header_v2".
    let needle = b"aorta_header\0";
    if bfbs.len() < needle.len() {
        return false;
    }
    bfbs.windows(needle.len()).any(|window| window == needle)
}

#[cfg(test)]
#[path = "../test/schema_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../test/schema_advanced_tests.rs"]
mod advanced_tests;
