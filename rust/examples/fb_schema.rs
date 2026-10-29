use aorta_sdk::{HeaderInjector, HeaderValues, SchemaMeta};
use flatbuffers::FlatBufferBuilder;
use std::marker::PhantomData;

pub trait FbSchemaSpec {
    fn schema_bfbs() -> &'static [u8];
    fn fixed_size() -> Option<usize>;
    fn root_type() -> &'static str;
}

pub struct FbSchema<T>(PhantomData<T>);

impl<T: FbSchemaSpec> SchemaMeta for FbSchema<T> {
    fn schema_bfbs() -> &'static [u8] {
        T::schema_bfbs()
    }

    fn fixed_size() -> Option<usize> {
        T::fixed_size()
    }

    fn root_type() -> Option<&'static str> {
        Some(T::root_type())
    }
}

impl<T: FbSchemaSpec> HeaderInjector for FbSchema<T> {
    type Header<'a> = super::aorta::sys::AortaHeader<'a>;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        values: &HeaderValues<'_>,
    ) -> flatbuffers::WIPOffset<Self::Header<'b>> {
        let node = builder.create_string(values.publisher_node);
        super::aorta::sys::AortaHeader::create(
            builder,
            &super::aorta::sys::AortaHeaderArgs {
                publish_stamp_ns: values.publish_stamp_ns,
                publisher_node: Some(node),
                sequence: values.sequence,
            },
        )
    }
}
