//! Hand-written FlatBuffers wire access for the
//! `aorta.ctx.OwnerConflictEvent` telemetry envelope. Mirrors
//! `message/schemas/system/ctx_owner_conflict.fbs`. Same hand-written rule
//! as `contextstore_fb.rs`: keep VOffsets in lockstep with the schema; only
//! ever append.
//!
//! Safety invariant for the `unsafe { _tab.get::<T>(VT_X, default) }`
//! blocks below mirrors `contextstore_fb.rs`: the `Table` is only reachable
//! via `flatbuffers::root::<T>(data)` which runs the verifier first.
//! Module-level `#[allow]` keeps the file mechanical.
#![allow(clippy::undocumented_unsafe_blocks)]

use flatbuffers::{
    FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer, Table, VOffsetT, Verifiable, Verifier,
    WIPOffset,
};

const VT_KEY: VOffsetT = 4;
const VT_SELF_NODE: VOffsetT = 6;
const VT_SELF_INSTANCE_ID: VOffsetT = 8;
const VT_FOREIGN_NODE: VOffsetT = 10;
const VT_FOREIGN_INSTANCE_ID: VOffsetT = 12;
const VT_FOREIGN_STAMP: VOffsetT = 14;
const VT_FOREIGN_SEQ: VOffsetT = 16;
const VT_OBSERVED_AT: VOffsetT = 18;

#[derive(Clone, Copy)]
pub struct OwnerConflictEvent<'a> {
    _tab: Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for OwnerConflictEvent<'a> {
    type Inner = OwnerConflictEvent<'a>;

    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: Table::new(buf, loc),
        }
    }
}

impl<'a> OwnerConflictEvent<'a> {
    #[inline]
    pub fn key(&self) -> Option<&'a str> {
        unsafe { self._tab.get::<ForwardsUOffset<&str>>(VT_KEY, None) }
    }

    #[inline]
    pub fn self_node(&self) -> Option<&'a str> {
        unsafe { self._tab.get::<ForwardsUOffset<&str>>(VT_SELF_NODE, None) }
    }

    #[inline]
    pub fn self_instance_id(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_SELF_INSTANCE_ID, None)
        }
    }

    #[inline]
    pub fn foreign_node(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_FOREIGN_NODE, None)
        }
    }

    #[inline]
    pub fn foreign_instance_id(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_FOREIGN_INSTANCE_ID, None)
        }
    }

    #[inline]
    pub fn foreign_stamp(&self) -> u64 {
        unsafe { self._tab.get::<u64>(VT_FOREIGN_STAMP, Some(0)) }.unwrap_or(0)
    }

    #[inline]
    pub fn foreign_seq(&self) -> u64 {
        unsafe { self._tab.get::<u64>(VT_FOREIGN_SEQ, Some(0)) }.unwrap_or(0)
    }

    #[inline]
    pub fn observed_at(&self) -> u64 {
        unsafe { self._tab.get::<u64>(VT_OBSERVED_AT, Some(0)) }.unwrap_or(0)
    }
}

pub fn root_as_owner_conflict_event(data: &[u8]) -> Option<OwnerConflictEvent<'_>> {
    flatbuffers::root::<OwnerConflictEvent>(data).ok()
}

#[allow(clippy::too_many_arguments)]
pub fn create_owner_conflict_event<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    key: Option<WIPOffset<&'a str>>,
    self_node: Option<WIPOffset<&'a str>>,
    self_instance_id: Option<WIPOffset<&'a str>>,
    foreign_node: Option<WIPOffset<&'a str>>,
    foreign_instance_id: Option<WIPOffset<&'a str>>,
    foreign_stamp: u64,
    foreign_seq: u64,
    observed_at: u64,
) -> WIPOffset<OwnerConflictEvent<'a>> {
    let start = builder.start_table();

    if let Some(off) = key {
        builder.push_slot_always::<WIPOffset<_>>(VT_KEY, off);
    }
    if let Some(off) = self_node {
        builder.push_slot_always::<WIPOffset<_>>(VT_SELF_NODE, off);
    }
    if let Some(off) = self_instance_id {
        builder.push_slot_always::<WIPOffset<_>>(VT_SELF_INSTANCE_ID, off);
    }
    if let Some(off) = foreign_node {
        builder.push_slot_always::<WIPOffset<_>>(VT_FOREIGN_NODE, off);
    }
    if let Some(off) = foreign_instance_id {
        builder.push_slot_always::<WIPOffset<_>>(VT_FOREIGN_INSTANCE_ID, off);
    }
    builder.push_slot::<u64>(VT_FOREIGN_STAMP, foreign_stamp, 0);
    builder.push_slot::<u64>(VT_FOREIGN_SEQ, foreign_seq, 0);
    builder.push_slot::<u64>(VT_OBSERVED_AT, observed_at, 0);

    let end = builder.end_table(start);
    WIPOffset::new(end.value())
}

impl<'a> Verifiable for OwnerConflictEvent<'a> {
    #[inline]
    fn run_verifier(v: &mut Verifier, pos: usize) -> core::result::Result<(), InvalidFlatbuffer> {
        let tv = v.visit_table(pos)?;
        tv.visit_field::<ForwardsUOffset<&str>>("key", VT_KEY, false)?
            .visit_field::<ForwardsUOffset<&str>>("self_node", VT_SELF_NODE, false)?
            .visit_field::<ForwardsUOffset<&str>>("self_instance_id", VT_SELF_INSTANCE_ID, false)?
            .visit_field::<ForwardsUOffset<&str>>("foreign_node", VT_FOREIGN_NODE, false)?
            .visit_field::<ForwardsUOffset<&str>>(
                "foreign_instance_id",
                VT_FOREIGN_INSTANCE_ID,
                false,
            )?
            .visit_field::<u64>("foreign_stamp", VT_FOREIGN_STAMP, false)?
            .visit_field::<u64>("foreign_seq", VT_FOREIGN_SEQ, false)?
            .visit_field::<u64>("observed_at", VT_OBSERVED_AT, false)?
            .finish();
        Ok(())
    }
}

/// Convenience encoder used by the conflict-detection path. Returns the
/// finished FlatBuffer bytes for an `OwnerConflictEvent` ready for `put` to
/// the telemetry stream.
#[allow(clippy::too_many_arguments)]
pub fn encode_owner_conflict_event(
    key: &str,
    self_node: &str,
    self_instance_id: &str,
    foreign_node: &str,
    foreign_instance_id: &str,
    foreign_stamp: u64,
    foreign_seq: u64,
    observed_at: u64,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let key_off = builder.create_string(key);
    let self_node_off = builder.create_string(self_node);
    let self_instance_off = builder.create_string(self_instance_id);
    let foreign_node_off = builder.create_string(foreign_node);
    let foreign_instance_off = builder.create_string(foreign_instance_id);
    let root = create_owner_conflict_event(
        &mut builder,
        Some(key_off),
        Some(self_node_off),
        Some(self_instance_off),
        Some(foreign_node_off),
        Some(foreign_instance_off),
        foreign_stamp,
        foreign_seq,
        observed_at,
    );
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

#[cfg(test)]
#[path = "../test/owner_conflict_fb_tests.rs"]
mod tests;
