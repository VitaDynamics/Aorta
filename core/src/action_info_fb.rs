//! Hand-written FlatBuffers wire access for the action-info table. Same
//! rationale as `contextstore_fb.rs`: every `unsafe { _tab.get::<T>(VT_X,
//! default) }` is sound because the `Table` only exists after
//! `flatbuffers::root::<T>(data)` runs the verifier. Module-level `#[allow]`
//! keeps the file mechanical.
#![allow(clippy::undocumented_unsafe_blocks)]

use flatbuffers::{
    FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer, Table, VOffsetT, Verifiable, Verifier,
    WIPOffset,
};

use crate::action::{DEFAULT_ACTION_DEDUP_TTL_MS, DEFAULT_ACTION_RESULT_TTL_MS};

const VT_ACTION_NAME: VOffsetT = 4;
const VT_SCHEMA_HASH_HEX: VOffsetT = 6;
const VT_SUPPORTS_CANCEL: VOffsetT = 8;
const VT_MAX_CONCURRENCY: VOffsetT = 10;
const VT_RESULT_TTL_MS: VOffsetT = 12;
const VT_DEDUP_TTL_MS: VOffsetT = 14;
const VT_SERVER_NODE_NAME: VOffsetT = 16;
const VT_SERVER_EPOCH: VOffsetT = 18;

#[derive(Clone, Copy)]
pub struct ActionInfo<'a> {
    _tab: Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for ActionInfo<'a> {
    type Inner = ActionInfo<'a>;

    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: Table::new(buf, loc),
        }
    }
}

impl<'a> ActionInfo<'a> {
    #[inline]
    pub fn action_name(&self) -> Option<&'a str> {
        unsafe { self._tab.get::<ForwardsUOffset<&str>>(VT_ACTION_NAME, None) }
    }

    #[inline]
    pub fn schema_hash_hex(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_SCHEMA_HASH_HEX, None)
        }
    }

    #[inline]
    pub fn supports_cancel(&self) -> bool {
        unsafe { self._tab.get::<bool>(VT_SUPPORTS_CANCEL, Some(true)) }.unwrap_or(true)
    }

    #[inline]
    pub fn max_concurrency(&self) -> u32 {
        unsafe { self._tab.get::<u32>(VT_MAX_CONCURRENCY, Some(0)) }.unwrap_or(0)
    }

    #[inline]
    pub fn result_ttl_ms(&self) -> u64 {
        unsafe {
            self._tab
                .get::<u64>(VT_RESULT_TTL_MS, Some(DEFAULT_ACTION_RESULT_TTL_MS))
        }
        .unwrap_or(DEFAULT_ACTION_RESULT_TTL_MS)
    }

    #[inline]
    pub fn dedup_ttl_ms(&self) -> u64 {
        unsafe {
            self._tab
                .get::<u64>(VT_DEDUP_TTL_MS, Some(DEFAULT_ACTION_DEDUP_TTL_MS))
        }
        .unwrap_or(DEFAULT_ACTION_DEDUP_TTL_MS)
    }

    #[inline]
    pub fn server_node_name(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_SERVER_NODE_NAME, None)
        }
    }

    #[inline]
    pub fn server_epoch(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_SERVER_EPOCH, None)
        }
    }
}

pub fn root_as_action_info(data: &[u8]) -> Option<ActionInfo<'_>> {
    flatbuffers::root::<ActionInfo>(data).ok()
}

#[allow(clippy::too_many_arguments)]
pub fn create_action_info<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    action_name: Option<WIPOffset<&'a str>>,
    schema_hash_hex: Option<WIPOffset<&'a str>>,
    supports_cancel: bool,
    max_concurrency: u32,
    result_ttl_ms: u64,
    dedup_ttl_ms: u64,
    server_node_name: Option<WIPOffset<&'a str>>,
    server_epoch: Option<WIPOffset<&'a str>>,
) -> WIPOffset<ActionInfo<'a>> {
    let start = builder.start_table();

    if let Some(off) = action_name {
        builder.push_slot_always::<WIPOffset<_>>(VT_ACTION_NAME, off);
    }
    if let Some(off) = schema_hash_hex {
        builder.push_slot_always::<WIPOffset<_>>(VT_SCHEMA_HASH_HEX, off);
    }
    builder.push_slot::<bool>(VT_SUPPORTS_CANCEL, supports_cancel, true);
    builder.push_slot::<u32>(VT_MAX_CONCURRENCY, max_concurrency, 0);
    builder.push_slot::<u64>(
        VT_RESULT_TTL_MS,
        result_ttl_ms,
        DEFAULT_ACTION_RESULT_TTL_MS,
    );
    builder.push_slot::<u64>(VT_DEDUP_TTL_MS, dedup_ttl_ms, DEFAULT_ACTION_DEDUP_TTL_MS);
    if let Some(off) = server_node_name {
        builder.push_slot_always::<WIPOffset<_>>(VT_SERVER_NODE_NAME, off);
    }
    if let Some(off) = server_epoch {
        builder.push_slot_always::<WIPOffset<_>>(VT_SERVER_EPOCH, off);
    }

    let end = builder.end_table(start);
    WIPOffset::new(end.value())
}

impl<'a> Verifiable for ActionInfo<'a> {
    #[inline]
    fn run_verifier(v: &mut Verifier, pos: usize) -> core::result::Result<(), InvalidFlatbuffer> {
        let tv = v.visit_table(pos)?;
        tv.visit_field::<ForwardsUOffset<&str>>("action_name", VT_ACTION_NAME, false)?
            .visit_field::<ForwardsUOffset<&str>>("schema_hash_hex", VT_SCHEMA_HASH_HEX, false)?
            .visit_field::<bool>("supports_cancel", VT_SUPPORTS_CANCEL, false)?
            .visit_field::<u32>("max_concurrency", VT_MAX_CONCURRENCY, false)?
            .visit_field::<u64>("result_ttl_ms", VT_RESULT_TTL_MS, false)?
            .visit_field::<u64>("dedup_ttl_ms", VT_DEDUP_TTL_MS, false)?
            .visit_field::<ForwardsUOffset<&str>>("server_node_name", VT_SERVER_NODE_NAME, false)?
            .visit_field::<ForwardsUOffset<&str>>("server_epoch", VT_SERVER_EPOCH, false)?
            .finish();
        Ok(())
    }
}
