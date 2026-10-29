use super::ensure_schema_bfbs;
use super::{builder_from_vec, RecyclableBuffer, RecyclePool};
use crate::header::{HeaderInjector, HeaderValues};
use aorta_core::schema::SchemaMeta;
use flatbuffers::{FlatBufferBuilder, Follow, WIPOffset};
use std::sync::{Arc, Mutex, OnceLock};

struct DummyMeta;

impl SchemaMeta for DummyMeta {
    fn schema_bfbs() -> &'static [u8] {
        &[1u8, 2u8, 3u8]
    }

    fn root_type() -> Option<&'static str> {
        Some("demo.DummyMeta")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordedValues {
    publish_stamp_ns: u64,
    publisher_node: String,
    sequence: u64,
}

static RECORDED: OnceLock<Mutex<Vec<RecordedValues>>> = OnceLock::new();

struct DummyHeader;

impl<'a> Follow<'a> for DummyHeader {
    type Inner = DummyHeader;

    unsafe fn follow(_buf: &'a [u8], _loc: usize) -> Self::Inner {
        DummyHeader
    }
}

struct DummyRoot;

impl<'a> Follow<'a> for DummyRoot {
    type Inner = DummyRoot;

    unsafe fn follow(_buf: &'a [u8], _loc: usize) -> Self::Inner {
        DummyRoot
    }
}

struct DummyMessage;

impl SchemaMeta for DummyMessage {
    fn schema_bfbs() -> &'static [u8] {
        &[1u8, 2u8, 3u8]
    }
}

impl HeaderInjector for DummyMessage {
    type Header<'a> = DummyHeader;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        values: &HeaderValues<'_>,
    ) -> flatbuffers::WIPOffset<Self::Header<'b>> {
        let recorded = RecordedValues {
            publish_stamp_ns: values.publish_stamp_ns,
            publisher_node: values.publisher_node.to_string(),
            sequence: values.sequence,
        };
        let store = RECORDED.get_or_init(|| Mutex::new(Vec::new()));
        store.lock().expect("lock").push(recorded);
        let start = builder.start_table();
        let end = builder.end_table(start);
        WIPOffset::new(end.value())
    }
}

struct FixedSizeMessage;

impl SchemaMeta for FixedSizeMessage {
    fn schema_bfbs() -> &'static [u8] {
        b"fixed_aorta_header_schema"
    }

    fn fixed_size() -> Option<usize> {
        Some(256)
    }
}

impl HeaderInjector for FixedSizeMessage {
    type Header<'a> = DummyHeader;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        _values: &HeaderValues<'_>,
    ) -> flatbuffers::WIPOffset<Self::Header<'b>> {
        let start = builder.start_table();
        let end = builder.end_table(start);
        WIPOffset::new(end.value())
    }
}

#[test]
fn ensure_schema_bfbs_inserts_metadata() {
    let mut options = aorta_core::publisher::PublisherOptions::default();
    ensure_schema_bfbs::<DummyMeta>(&mut options).expect("schema");
    assert_eq!(options.schema_bfbs.as_deref(), Some(&[1u8, 2u8, 3u8][..]));
    assert_eq!(options.schema_root_type.as_deref(), Some("demo.DummyMeta"));
}

#[test]
fn ensure_schema_bfbs_keeps_existing() {
    let mut options = aorta_core::publisher::PublisherOptions::default();
    options.schema_bfbs = Some(vec![9u8]);
    ensure_schema_bfbs::<DummyMeta>(&mut options).expect("schema");
    assert_eq!(options.schema_bfbs.as_deref(), Some(&[9u8][..]));
    assert!(options.schema_root_type.is_none());
}

#[test]
fn header_injection_records_values() {
    let store = RECORDED.get_or_init(|| Mutex::new(Vec::new()));
    store.lock().expect("lock").clear();

    let mut builder = FlatBufferBuilder::new();
    let values = HeaderValues {
        publish_stamp_ns: 42,
        publisher_node: "node",
        sequence: 7,
    };

    // Inline header injection (same as publish() does)
    let _header = DummyMessage::build_header(&mut builder, &values);
    let start = builder.start_table();
    let end = builder.end_table(start);
    let root = WIPOffset::<DummyRoot>::new(end.value());
    builder.finish(root, None);

    assert!(!builder.finished_data().is_empty());
    let recorded = store.lock().expect("lock");
    assert_eq!(
        recorded.last(),
        Some(&RecordedValues {
            publish_stamp_ns: 42,
            publisher_node: "node".to_string(),
            sequence: 7,
        })
    );
}

#[test]
fn builder_config_prefers_max_message_size() {
    let mut options = aorta_core::publisher::PublisherOptions::default();
    options.max_message_size = Some(1024);
    let config =
        super::Publisher::<FixedSizeMessage>::builder_config_from_options(&options, "test/topic");
    assert_eq!(config.base_capacity, 1024);
}

#[test]
fn builder_config_uses_fixed_size_when_available() {
    let options = aorta_core::publisher::PublisherOptions::default();
    let config =
        super::Publisher::<FixedSizeMessage>::builder_config_from_options(&options, "test/topic");
    assert_eq!(config.base_capacity, 256);
}

#[test]
fn builder_config_defaults_for_variable_size() {
    let options = aorta_core::publisher::PublisherOptions::default();
    let config =
        super::Publisher::<DummyMessage>::builder_config_from_options(&options, "test/topic");
    assert_eq!(config.base_capacity, super::DEFAULT_VAR_CAPACITY);
}

#[test]
fn builder_config_threads_publish_timestamp_flag() {
    let mut options = aorta_core::publisher::PublisherOptions::default();
    options.include_publish_timestamp = false;
    let config =
        super::Publisher::<DummyMessage>::builder_config_from_options(&options, "test/topic");
    assert!(!config.include_publish_timestamp);

    let default_config = super::Publisher::<DummyMessage>::builder_config_from_options(
        &aorta_core::publisher::PublisherOptions::default(),
        "test/topic",
    );
    assert!(default_config.include_publish_timestamp);
}

// ---------------------------------------------------------------------------
// RecyclePool tests
// ---------------------------------------------------------------------------

#[test]
fn recycle_pool_push_pop_returns_same_capacity() {
    let pool = RecyclePool::new(4096);
    let buf = vec![0u8; 1024];
    let original_cap = buf.capacity();
    let original_len = buf.len();
    pool.push(buf);
    assert_eq!(pool.depth(), 1);

    let popped = pool.pop().expect("pool should not be empty");
    assert_eq!(popped.capacity(), original_cap);
    assert_eq!(popped.len(), original_len);
    assert_eq!(pool.depth(), 0);
}

#[test]
fn recycle_pool_pop_empty_returns_none() {
    let pool = RecyclePool::new(4096);
    assert!(pool.pop().is_none());
    assert_eq!(pool.depth(), 0);
}

#[test]
fn recycle_pool_lifo_ordering() {
    let pool = RecyclePool::new(4096);
    let buf_a = Vec::<u8>::with_capacity(64);
    let cap_a = buf_a.capacity();
    let buf_b = Vec::<u8>::with_capacity(256);
    let cap_b = buf_b.capacity();

    pool.push(buf_a); // bottom
    pool.push(buf_b); // top
    assert_eq!(pool.depth(), 2);

    // LIFO: pop returns B first, then A
    let first = pool.pop().expect("first pop");
    assert_eq!(first.capacity(), cap_b);
    let second = pool.pop().expect("second pop");
    assert_eq!(second.capacity(), cap_a);
    assert!(pool.pop().is_none());
}

#[test]
fn recycle_pool_skips_small_buffers() {
    let pool = RecyclePool::new(4096);
    // Vec with capacity < 16 (MIN_RECYCLE_CAPACITY) should be silently dropped
    let tiny = Vec::<u8>::with_capacity(8);
    pool.push(tiny);
    assert_eq!(pool.depth(), 0);
    assert!(pool.pop().is_none());
}

#[test]
fn recycle_pool_concurrent_mpmc_stress() {
    // Regression test for the MPMC-unsafe recycle pool: pops run
    // concurrently on every publisher thread while Zenoh drop threads push,
    // so the pool must be MPMC-safe.  The old intrusive Treiber-stack
    // implementation was ABA-unsound under concurrent pop and could hand
    // the same allocation to two owners; the live-set assertion below is
    // exactly that aliasing failure mode.
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::thread;

    const THREADS: usize = 8;
    const ITERS: usize = 2000;

    let pool = Arc::new(RecyclePool::new(4096));
    let live: Arc<Mutex<HashSet<usize>>> = Arc::new(Mutex::new(HashSet::new()));

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let pool = Arc::clone(&pool);
        let live = Arc::clone(&live);
        handles.push(thread::spawn(move || {
            for i in 0..ITERS {
                let mut buf = pool.pop().unwrap_or_else(|| vec![0u8; 1024]);
                let addr = buf.as_ptr() as usize;
                assert!(
                    live.lock().unwrap().insert(addr),
                    "pool handed one allocation to two concurrent owners"
                );
                // Touch the buffer the way a FlatBufferBuilder would.
                let fill = (t.wrapping_mul(31).wrapping_add(i)) as u8;
                buf.fill(fill);
                assert!(buf.iter().all(|&b| b == fill));
                assert!(live.lock().unwrap().remove(&addr));
                pool.push(buf);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("stress thread panicked");
    }
    // Depth cap must hold after arbitrary interleaving.
    assert!(pool.depth() <= 8);
}

#[test]
fn recycle_pool_drop_frees_remaining() {
    let pool = RecyclePool::new(4096);
    pool.push(Vec::<u8>::with_capacity(128));
    pool.push(Vec::<u8>::with_capacity(256));
    assert_eq!(pool.depth(), 2);
    // Drop the pool; if this leaks, Miri/ASAN would catch it.
    drop(pool);
}

// ---------------------------------------------------------------------------
// RecyclableBuffer tests
// ---------------------------------------------------------------------------

#[test]
fn recyclable_buffer_asref_returns_correct_slice() {
    let pool = Arc::new(RecyclePool::new(4096));

    // Build a small FlatBuffer, collapse it, and verify the slice.
    let mut builder = FlatBufferBuilder::with_capacity(128);
    let start = builder.start_table();
    let end = builder.end_table(start);
    let root = WIPOffset::<flatbuffers::ForwardsUOffset<flatbuffers::Table>>::new(end.value());
    builder.finish(root, None);
    let expected_data = builder.finished_data().to_vec();

    let (vec, offset) = builder.collapse();
    let rb = RecyclableBuffer {
        vec,
        offset,
        pool: Arc::clone(&pool),
    };

    assert_eq!(rb.as_ref(), &expected_data[..]);
}

#[test]
fn recyclable_buffer_drop_returns_vec_to_pool() {
    let pool = Arc::new(RecyclePool::new(4096));
    assert_eq!(pool.depth(), 0);

    let mut buf = vec![0u8; 512];
    buf[400..].fill(0xAB);
    let rb = RecyclableBuffer {
        vec: buf,
        offset: 400,
        pool: Arc::clone(&pool),
    };
    drop(rb);

    assert_eq!(pool.depth(), 1);
    let recovered = pool.pop().expect("should recover buffer");
    assert!(recovered.capacity() >= 512);
    assert!(recovered[400..].iter().all(|byte| *byte == 0));
}

#[test]
fn collapse_produces_valid_flatbuffer_data() {
    // Build a FlatBuffer with a vector of known bytes, collapse, verify.
    let mut builder = FlatBufferBuilder::with_capacity(256);
    let data: Vec<u8> = (0..64).collect();
    let vec_offset = builder.create_vector(&data);
    builder.finish(vec_offset, None);

    let finished = builder.finished_data().to_vec();
    let (vec, offset) = builder.collapse();

    // The region vec[offset..] must match finished_data().
    assert_eq!(&vec[offset..], &finished[..]);
    assert!(!finished.is_empty());
}

#[test]
fn recycled_builder_handles_nonempty_then_empty_vector() {
    let mut builder = builder_from_vec(vec![0u8; 256]);
    let data = builder.create_vector(&[7u8; 64]);
    builder.finish(data, None);
    let (mut vec, _) = builder.collapse();
    vec.clear();

    let mut builder = builder_from_vec(vec);
    let empty = builder.create_vector::<u8>(&[]);
    builder.finish(empty, None);

    let parsed = flatbuffers::root::<flatbuffers::Vector<'_, u8>>(builder.finished_data())
        .expect("recycled empty vector must verify");
    assert!(parsed.is_empty());
}

#[test]
fn byte_string_offset_is_wire_compatible_with_vector_u8() {
    let data = [1u8, 2, 3, 255];
    let mut builder = FlatBufferBuilder::new();
    let offset =
        WIPOffset::<flatbuffers::Vector<'_, u8>>::new(builder.create_byte_string(&data).value());
    builder.finish(offset, None);

    let parsed = flatbuffers::root::<flatbuffers::Vector<'_, u8>>(builder.finished_data())
        .expect("vector root");
    assert_eq!(parsed.len(), data.len());
    assert_eq!(parsed.bytes(), &data);
}

#[test]
fn byte_string_offset_is_wire_compatible_with_vector_i8() {
    let data = [-1i8, 0, 1, 127];
    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len()) };
    let mut builder = FlatBufferBuilder::new();
    let offset =
        WIPOffset::<flatbuffers::Vector<'_, i8>>::new(builder.create_byte_string(bytes).value());
    builder.finish(offset, None);

    let parsed = flatbuffers::root::<flatbuffers::Vector<'_, i8>>(builder.finished_data())
        .expect("vector root");
    assert_eq!(parsed.len(), data.len());
    let values: Vec<i8> = (0..parsed.len()).map(|idx| parsed.get(idx)).collect();
    assert_eq!(values, data);
}
