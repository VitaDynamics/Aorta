//! Hand-written FlatBuffers wire access for the ContextStore `ContextValue`
//! table. Mirrors `message/schemas/system/contextstore.fbs` and follows the
//! same convention as `schema_by_key_fb.rs` so the core crate can encode and
//! decode context values without a `flatc`-driven build step.
//!
//! The wire layout MUST stay in lockstep with the `.fbs` schema. When the
//! schema gains a new field, append the new VOffset here at the next
//! available even slot — never reorder existing slots.
//!
//! # Safety invariant
//!
//! Every `unsafe { self._tab.get::<T>(VOFFSET, default) }` in this module is
//! sound because the `Table` is only constructable via
//! `flatbuffers::root::<T>(data)` — which runs `Verifiable::run_verifier`
//! and rejects malformed buffers before `_tab` exists. `Table::get` is
//! `unsafe` purely because the verifier-vs-VOffset pairing is a caller
//! invariant the type system can't express, and the verifier IS the proof.
//! The VOffsets are the module-private `VT_*` constants above and stay in
//! lockstep with the `.fbs` schema. Module-level `#[allow]` rather than
//! per-block annotation keeps the file mechanical (every block uses the
//! same one-line `_tab.get::<T>(VT_X, default)` shape).
#![allow(clippy::undocumented_unsafe_blocks)]

use flatbuffers::{
    FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer, Table, VOffsetT, Verifiable, Verifier,
    WIPOffset,
};

const VT_KIND: VOffsetT = 4;
const VT_BOOL_VALUE: VOffsetT = 6;
const VT_I64_VALUE: VOffsetT = 8;
const VT_U64_VALUE: VOffsetT = 10;
const VT_F64_VALUE: VOffsetT = 12;
const VT_STRING_VALUE: VOffsetT = 14;
const VT_STAMP_NS: VOffsetT = 16;
const VT_EXPIRES_AT_NS: VOffsetT = 18;
const VT_SOURCE_NODE: VOffsetT = 20;
const VT_SOURCE_INSTANCE_ID: VOffsetT = 22;
const VT_SEQUENCE: VOffsetT = 24;

/// Discriminator for the active value field. Mirrors the `ContextKind` enum
/// in `contextstore.fbs`.
#[repr(i8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ContextKind {
    None = 0,
    Bool = 1,
    I64 = 2,
    U64 = 3,
    F64 = 4,
    String = 5,
}

impl ContextKind {
    pub fn from_byte(b: i8) -> Option<Self> {
        match b {
            0 => Some(Self::None),
            1 => Some(Self::Bool),
            2 => Some(Self::I64),
            3 => Some(Self::U64),
            4 => Some(Self::F64),
            5 => Some(Self::String),
            _ => None,
        }
    }

    pub fn as_byte(self) -> i8 {
        self as i8
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::Bool => "Bool",
            Self::I64 => "I64",
            Self::U64 => "U64",
            Self::F64 => "F64",
            Self::String => "String",
        }
    }
}

#[derive(Clone, Copy)]
pub struct ContextValue<'a> {
    _tab: Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for ContextValue<'a> {
    type Inner = ContextValue<'a>;

    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: Table::new(buf, loc),
        }
    }
}

impl<'a> ContextValue<'a> {
    #[inline]
    pub fn kind_raw(&self) -> i8 {
        unsafe { self._tab.get::<i8>(VT_KIND, Some(0)) }.unwrap_or(0)
    }

    #[inline]
    pub fn kind(&self) -> ContextKind {
        ContextKind::from_byte(self.kind_raw()).unwrap_or(ContextKind::None)
    }

    #[inline]
    pub fn bool_value(&self) -> bool {
        unsafe { self._tab.get::<bool>(VT_BOOL_VALUE, Some(false)) }.unwrap_or(false)
    }

    #[inline]
    pub fn i64_value(&self) -> i64 {
        unsafe { self._tab.get::<i64>(VT_I64_VALUE, Some(0)) }.unwrap_or(0)
    }

    #[inline]
    pub fn u64_value(&self) -> u64 {
        unsafe { self._tab.get::<u64>(VT_U64_VALUE, Some(0)) }.unwrap_or(0)
    }

    #[inline]
    pub fn f64_value(&self) -> f64 {
        unsafe { self._tab.get::<f64>(VT_F64_VALUE, Some(0.0)) }.unwrap_or(0.0)
    }

    #[inline]
    pub fn string_value(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_STRING_VALUE, None)
        }
    }

    #[inline]
    pub fn stamp_ns(&self) -> u64 {
        unsafe { self._tab.get::<u64>(VT_STAMP_NS, Some(0)) }.unwrap_or(0)
    }

    #[inline]
    pub fn expires_at_ns(&self) -> u64 {
        unsafe { self._tab.get::<u64>(VT_EXPIRES_AT_NS, Some(0)) }.unwrap_or(0)
    }

    #[inline]
    pub fn source_node(&self) -> Option<&'a str> {
        unsafe { self._tab.get::<ForwardsUOffset<&str>>(VT_SOURCE_NODE, None) }
    }

    #[inline]
    pub fn source_instance_id(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_SOURCE_INSTANCE_ID, None)
        }
    }

    #[inline]
    pub fn sequence(&self) -> u64 {
        unsafe { self._tab.get::<u64>(VT_SEQUENCE, Some(0)) }.unwrap_or(0)
    }
}

pub fn root_as_context_value(data: &[u8]) -> Option<ContextValue<'_>> {
    flatbuffers::root::<ContextValue>(data).ok()
}

/// Construct a `ContextValue` table. Caller is responsible for selecting the
/// correct typed-field offset for the chosen `kind`; pass `None` for fields
/// that are not active. Numeric fields default to zero on the wire when not
/// pushed, so leaving them at default is also acceptable for "not set".
#[allow(clippy::too_many_arguments)]
pub fn create_context_value<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    kind: ContextKind,
    bool_value: bool,
    i64_value: i64,
    u64_value: u64,
    f64_value: f64,
    string_value: Option<WIPOffset<&'a str>>,
    stamp_ns: u64,
    expires_at_ns: u64,
    source_node: Option<WIPOffset<&'a str>>,
    source_instance_id: Option<WIPOffset<&'a str>>,
    sequence: u64,
) -> WIPOffset<ContextValue<'a>> {
    let start = builder.start_table();

    builder.push_slot::<i8>(VT_KIND, kind.as_byte(), 0);

    match kind {
        ContextKind::Bool => {
            builder.push_slot::<bool>(VT_BOOL_VALUE, bool_value, false);
        }
        ContextKind::I64 => {
            builder.push_slot::<i64>(VT_I64_VALUE, i64_value, 0);
        }
        ContextKind::U64 => {
            builder.push_slot::<u64>(VT_U64_VALUE, u64_value, 0);
        }
        ContextKind::F64 => {
            builder.push_slot::<f64>(VT_F64_VALUE, f64_value, 0.0);
        }
        ContextKind::String => {
            if let Some(off) = string_value {
                builder.push_slot_always::<WIPOffset<_>>(VT_STRING_VALUE, off);
            }
        }
        ContextKind::None => {}
    }

    builder.push_slot::<u64>(VT_STAMP_NS, stamp_ns, 0);
    builder.push_slot::<u64>(VT_EXPIRES_AT_NS, expires_at_ns, 0);
    if let Some(off) = source_node {
        builder.push_slot_always::<WIPOffset<_>>(VT_SOURCE_NODE, off);
    }
    if let Some(off) = source_instance_id {
        builder.push_slot_always::<WIPOffset<_>>(VT_SOURCE_INSTANCE_ID, off);
    }
    builder.push_slot::<u64>(VT_SEQUENCE, sequence, 0);

    let end = builder.end_table(start);
    WIPOffset::new(end.value())
}

impl<'a> Verifiable for ContextValue<'a> {
    #[inline]
    fn run_verifier(v: &mut Verifier, pos: usize) -> core::result::Result<(), InvalidFlatbuffer> {
        let tv = v.visit_table(pos)?;
        tv.visit_field::<i8>("kind", VT_KIND, false)?
            .visit_field::<bool>("bool_value", VT_BOOL_VALUE, false)?
            .visit_field::<i64>("i64_value", VT_I64_VALUE, false)?
            .visit_field::<u64>("u64_value", VT_U64_VALUE, false)?
            .visit_field::<f64>("f64_value", VT_F64_VALUE, false)?
            .visit_field::<ForwardsUOffset<&str>>("string_value", VT_STRING_VALUE, false)?
            .visit_field::<u64>("stamp_ns", VT_STAMP_NS, false)?
            .visit_field::<u64>("expires_at_ns", VT_EXPIRES_AT_NS, false)?
            .visit_field::<ForwardsUOffset<&str>>("source_node", VT_SOURCE_NODE, false)?
            .visit_field::<ForwardsUOffset<&str>>(
                "source_instance_id",
                VT_SOURCE_INSTANCE_ID,
                false,
            )?
            .visit_field::<u64>("sequence", VT_SEQUENCE, false)?
            .finish();
        Ok(())
    }
}

/// All metadata carried alongside a context value. Used as the in-memory
/// representation of a decoded `ContextValue` in higher layers (e.g. the
/// owner-side latest-value cache and `ContextRead<T>` results).
#[derive(Clone, Debug, PartialEq)]
pub struct ContextValueMeta {
    pub stamp_ns: u64,
    pub expires_at_ns: u64,
    pub source_node: String,
    pub source_instance_id: String,
    pub sequence: u64,
}

/// Convenience: build a complete `ContextValue` payload from a typed value
/// plus metadata. Returns the finished FlatBuffer bytes ready to put on the
/// wire.
pub fn encode_value_bool(value: bool, meta: &ContextValueMeta) -> Vec<u8> {
    encode_inner(ContextKind::Bool, value, 0, 0, 0.0, None, meta)
}

pub fn encode_value_i64(value: i64, meta: &ContextValueMeta) -> Vec<u8> {
    encode_inner(ContextKind::I64, false, value, 0, 0.0, None, meta)
}

pub fn encode_value_u64(value: u64, meta: &ContextValueMeta) -> Vec<u8> {
    encode_inner(ContextKind::U64, false, 0, value, 0.0, None, meta)
}

pub fn encode_value_f64(value: f64, meta: &ContextValueMeta) -> Vec<u8> {
    encode_inner(ContextKind::F64, false, 0, 0, value, None, meta)
}

pub fn encode_value_string(value: &str, meta: &ContextValueMeta) -> Vec<u8> {
    encode_inner(ContextKind::String, false, 0, 0, 0.0, Some(value), meta)
}

fn encode_inner(
    kind: ContextKind,
    bool_value: bool,
    i64_value: i64,
    u64_value: u64,
    f64_value: f64,
    string_value: Option<&str>,
    meta: &ContextValueMeta,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let s_off = string_value.map(|s| builder.create_string(s));
    let node_off = builder.create_string(&meta.source_node);
    let instance_off = builder.create_string(&meta.source_instance_id);
    let root = create_context_value(
        &mut builder,
        kind,
        bool_value,
        i64_value,
        u64_value,
        f64_value,
        s_off,
        meta.stamp_ns,
        meta.expires_at_ns,
        Some(node_off),
        Some(instance_off),
        meta.sequence,
    );
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

/// BFBS (binary reflection schema) for `aorta.ctx.ContextValue`, re-exported
/// from the committed `contextstore_bfbs_generated` module. All context values
/// share this single schema, so the recorder embeds it directly to write a
/// typed `flatbuffer` MCAP channel (parseable by Foxglove) instead of the
/// opaque `aorta_raw_bytes` encoding. Regenerate via
/// `message/tools/regen_telemetry_fb.py`; `core/build.rs` drift-tests it.
pub use crate::contextstore_bfbs_generated::CONTEXT_VALUE_BFBS;

/// SHA-256 of the committed `ContextValue` BFBS. Cheap; recomputed on call.
/// All context values share this one schema, so the recorder attaches this hash
/// to every typed ctx MCAP channel.
pub fn context_value_schema_hash() -> crate::schema::SchemaHash {
    crate::schema::SchemaHash::from_bfbs_bytes(CONTEXT_VALUE_BFBS)
}

#[cfg(test)]
#[path = "../test/contextstore_codec_tests.rs"]
mod tests;
