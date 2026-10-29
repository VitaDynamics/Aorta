//! Hand-written FlatBuffers wire access for the schema-by-key table. Same
//! rationale as `contextstore_fb.rs`: every `unsafe { _tab.get::<T>(VT_X,
//! default) }` is sound because the `Table` only exists after
//! `flatbuffers::root::<T>(data)` runs the verifier. Module-level `#[allow]`
//! keeps the file mechanical.
#![allow(clippy::undocumented_unsafe_blocks)]

use flatbuffers::{
    FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer, Table, VOffsetT, Vector, Verifiable,
    Verifier, WIPOffset,
};

const VT_KIND: VOffsetT = 4;
const VT_HASH_HEX: VOffsetT = 6;
const VT_BFBS: VOffsetT = 8;
const VT_SERVICE_HASH_HEX: VOffsetT = 10;
const VT_SERVICE_BFBS: VOffsetT = 12;
const VT_REQUEST_ROOT_TYPE: VOffsetT = 14;
const VT_RESPONSE_ROOT_TYPE: VOffsetT = 16;
const VT_TOPIC_ROOT_TYPE: VOffsetT = 18;

#[derive(Clone, Copy)]
pub struct SchemaByKey<'a> {
    _tab: Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for SchemaByKey<'a> {
    type Inner = SchemaByKey<'a>;

    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: Table::new(buf, loc),
        }
    }
}

impl<'a> SchemaByKey<'a> {
    #[inline]
    pub fn kind(&self) -> u8 {
        unsafe { self._tab.get::<u8>(VT_KIND, Some(0)) }.unwrap_or(0)
    }

    #[inline]
    pub fn hash_hex(&self) -> Option<&'a str> {
        unsafe { self._tab.get::<ForwardsUOffset<&str>>(VT_HASH_HEX, None) }
    }

    #[inline]
    pub fn bfbs(&self) -> Option<&'a [u8]> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(VT_BFBS, None)
        }
        .map(|v| v.bytes())
    }

    #[inline]
    pub fn service_hash_hex(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_SERVICE_HASH_HEX, None)
        }
    }

    #[inline]
    pub fn service_bfbs(&self) -> Option<&'a [u8]> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(VT_SERVICE_BFBS, None)
        }
        .map(|v| v.bytes())
    }

    #[inline]
    pub fn request_root_type(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_REQUEST_ROOT_TYPE, None)
        }
    }

    #[inline]
    pub fn response_root_type(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_RESPONSE_ROOT_TYPE, None)
        }
    }

    #[inline]
    pub fn topic_root_type(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(VT_TOPIC_ROOT_TYPE, None)
        }
    }
}

pub fn root_as_schema_by_key(data: &[u8]) -> Option<SchemaByKey<'_>> {
    flatbuffers::root::<SchemaByKey>(data).ok()
}

pub fn create_schema_by_key<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    kind: u8,
    hash_hex: Option<WIPOffset<&'a str>>,
    bfbs: Option<WIPOffset<Vector<'a, u8>>>,
    service_hash_hex: Option<WIPOffset<&'a str>>,
    service_bfbs: Option<WIPOffset<Vector<'a, u8>>>,
    request_root_type: Option<WIPOffset<&'a str>>,
    response_root_type: Option<WIPOffset<&'a str>>,
    topic_root_type: Option<WIPOffset<&'a str>>,
) -> WIPOffset<SchemaByKey<'a>> {
    let start = builder.start_table();

    builder.push_slot::<u8>(VT_KIND, kind, 0);
    if let Some(off) = hash_hex {
        builder.push_slot_always::<WIPOffset<_>>(VT_HASH_HEX, off);
    }
    if let Some(off) = bfbs {
        builder.push_slot_always::<WIPOffset<_>>(VT_BFBS, off);
    }

    if let Some(off) = service_hash_hex {
        builder.push_slot_always::<WIPOffset<_>>(VT_SERVICE_HASH_HEX, off);
    }
    if let Some(off) = service_bfbs {
        builder.push_slot_always::<WIPOffset<_>>(VT_SERVICE_BFBS, off);
    }

    if let Some(off) = request_root_type {
        builder.push_slot_always::<WIPOffset<_>>(VT_REQUEST_ROOT_TYPE, off);
    }
    if let Some(off) = response_root_type {
        builder.push_slot_always::<WIPOffset<_>>(VT_RESPONSE_ROOT_TYPE, off);
    }
    if let Some(off) = topic_root_type {
        builder.push_slot_always::<WIPOffset<_>>(VT_TOPIC_ROOT_TYPE, off);
    }

    let end = builder.end_table(start);
    WIPOffset::new(end.value())
}

impl<'a> Verifiable for SchemaByKey<'a> {
    #[inline]
    fn run_verifier(v: &mut Verifier, pos: usize) -> core::result::Result<(), InvalidFlatbuffer> {
        let tv = v.visit_table(pos)?;
        tv.visit_field::<u8>("kind", VT_KIND, false)?
            .visit_field::<ForwardsUOffset<&str>>("hash_hex", VT_HASH_HEX, false)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>("bfbs", VT_BFBS, false)?
            .visit_field::<ForwardsUOffset<&str>>("service_hash_hex", VT_SERVICE_HASH_HEX, false)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>("service_bfbs", VT_SERVICE_BFBS, false)?
            .visit_field::<ForwardsUOffset<&str>>("request_root_type", VT_REQUEST_ROOT_TYPE, false)?
            .visit_field::<ForwardsUOffset<&str>>(
                "response_root_type",
                VT_RESPONSE_ROOT_TYPE,
                false,
            )?
            .visit_field::<ForwardsUOffset<&str>>("topic_root_type", VT_TOPIC_ROOT_TYPE, false)?
            .finish();
        Ok(())
    }
}

#[cfg(test)]
#[path = "../test/schema_by_key_fb_tests.rs"]
mod tests;
