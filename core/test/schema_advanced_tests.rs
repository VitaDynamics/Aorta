use super::*;
use crate::test_support::{allow_schema_env_guard, ENV_LOCK};

#[test]
fn schema_hash_from_bytes_roundtrip() {
    let bytes = [0u8; 32];
    let hash = SchemaHash::from_bytes(bytes);
    assert_eq!(hash.as_bytes(), &bytes);
}

#[test]
fn schema_hash_from_bfbs_deterministic() {
    let data = b"test schema data";
    let hash1 = SchemaHash::from_bfbs_bytes(data);
    let hash2 = SchemaHash::from_bfbs_bytes(data);
    assert_eq!(hash1.as_bytes(), hash2.as_bytes());
}

#[test]
fn schema_hash_different_data_different_hash() {
    let data1 = b"data one";
    let data2 = b"data two";
    let hash1 = SchemaHash::from_bfbs_bytes(data1);
    let hash2 = SchemaHash::from_bfbs_bytes(data2);
    assert_ne!(hash1.as_bytes(), hash2.as_bytes());
}

#[test]
fn schema_hash_to_hex_format() {
    let mut bytes = [0u8; 32];
    bytes[0] = 0xAB;
    bytes[1] = 0xCD;
    bytes[31] = 0xEF;

    let hash = SchemaHash::from_bytes(bytes);
    let hex = hash.to_hex();

    assert_eq!(hex.len(), 64);
    assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(&hex[0..2], "ab");
    assert_eq!(&hex[2..4], "cd");
    assert_eq!(&hex[62..64], "ef");
}

#[test]
fn schema_hash_to_hex_bytes_format() {
    let bytes = [0xABu8, 0xCD, 0xEF, 0x00];
    let mut full_bytes = [0u8; 32];
    full_bytes[0..4].copy_from_slice(&bytes);

    let hash = SchemaHash::from_bytes(full_bytes);
    let hex_bytes = hash.to_hex_bytes();

    assert_eq!(hex_bytes.len(), 64);
    assert_eq!(&hex_bytes[0..2], b"ab");
    assert_eq!(&hex_bytes[2..4], b"cd");
}

#[test]
fn schema_hash_from_hex_str_valid() {
    let hex = "0000000000000000000000000000000000000000000000000000000000000000";
    let hash = SchemaHash::from_hex_str(hex);
    assert!(hash.is_some());

    let hash = hash.unwrap();
    assert_eq!(hash.as_bytes(), &[0u8; 32]);
}

#[test]
fn schema_hash_from_hex_str_invalid_length() {
    let hex_short = "0000";
    assert!(SchemaHash::from_hex_str(hex_short).is_none());

    let hex_long = "0".repeat(100);
    assert!(SchemaHash::from_hex_str(&hex_long).is_none());
}

#[test]
fn schema_hash_from_hex_str_invalid_chars() {
    let hex_invalid = "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg";
    assert!(SchemaHash::from_hex_str(hex_invalid).is_none());
}

#[test]
fn schema_hash_from_hex_str_uppercase() {
    let hex = "ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890";
    assert!(SchemaHash::from_hex_str(hex).is_none()); // Should reject uppercase
}

#[test]
fn schema_hash_from_hex_bytes_valid() {
    let hex_bytes = b"0000000000000000000000000000000000000000000000000000000000000000";
    let hash = SchemaHash::from_hex_bytes(hex_bytes);
    assert!(hash.is_some());
}

#[test]
fn schema_hash_from_hex_bytes_wrong_length() {
    let bytes = b"0000";
    assert!(SchemaHash::from_hex_bytes(bytes).is_none());
}

#[test]
fn schema_hash_clone() {
    let hash = SchemaHash::from_bfbs_bytes(b"test");
    let cloned = hash;
    assert_eq!(hash.as_bytes(), cloned.as_bytes());
}

#[test]
fn schema_hash_copy() {
    let hash = SchemaHash::from_bfbs_bytes(b"test");
    let copied = hash;
    assert_eq!(hash.as_bytes(), copied.as_bytes());
}

#[test]
fn schema_hash_eq() {
    let hash1 = SchemaHash::from_bfbs_bytes(b"test");
    let hash2 = SchemaHash::from_bfbs_bytes(b"test");
    let hash3 = SchemaHash::from_bfbs_bytes(b"different");

    assert_eq!(hash1, hash2);
    assert_ne!(hash1, hash3);
}

#[test]
fn schema_hash_hashable() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    let hash1 = SchemaHash::from_bfbs_bytes(b"test1");
    let hash2 = SchemaHash::from_bfbs_bytes(b"test2");
    let hash3 = SchemaHash::from_bfbs_bytes(b"test1"); // Same as hash1

    set.insert(hash1);
    set.insert(hash2);
    set.insert(hash3); // Should not increase size (duplicate)

    assert_eq!(set.len(), 2);
}

#[test]
fn schema_hash_display() {
    let hash = SchemaHash::from_bfbs_bytes(b"test");
    let display = format!("{}", hash);
    assert_eq!(display.len(), 64);
    assert!(display.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn schema_hash_debug() {
    let hash = SchemaHash::from_bfbs_bytes(b"test");
    let debug = format!("{:?}", hash);
    assert!(debug.contains("SchemaHash"));
    assert!(debug.contains("sha256"));
}

#[test]
fn schema_attach_policy_from_env_always() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_schema_env_guard();
    std::env::set_var("AORTA_SCHEMA_ATTACH", "always");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Always);
    std::env::remove_var("AORTA_SCHEMA_ATTACH");
}

#[test]
fn schema_attach_policy_from_env_never() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_schema_env_guard();
    std::env::set_var("AORTA_SCHEMA_ATTACH", "never");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Never);
    std::env::remove_var("AORTA_SCHEMA_ATTACH");
}

#[test]
fn schema_attach_policy_from_env_unknown() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_schema_env_guard();
    std::env::set_var("AORTA_SCHEMA_ATTACH", "unknown_value");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Never);
    std::env::remove_var("AORTA_SCHEMA_ATTACH");
}

#[test]
fn schema_attach_policy_from_env_empty() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_schema_env_guard();
    std::env::set_var("AORTA_SCHEMA_ATTACH", "");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Never);
    std::env::remove_var("AORTA_SCHEMA_ATTACH");
}

#[test]
fn schema_attach_policy_from_env_case_insensitive() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_schema_env_guard();

    std::env::set_var("AORTA_SCHEMA_ATTACH", "ALWAYS");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Always);

    std::env::set_var("AORTA_SCHEMA_ATTACH", "Always");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Always);

    std::env::set_var("AORTA_SCHEMA_ATTACH", "NeVeR");
    assert_eq!(SchemaAttachPolicy::from_env(), SchemaAttachPolicy::Never);

    std::env::remove_var("AORTA_SCHEMA_ATTACH");
}

#[test]
fn schema_attach_policy_resolve_inherit_parent() {
    assert_eq!(
        SchemaAttachPolicy::Inherit.resolve(SchemaAttachPolicy::Always),
        SchemaAttachPolicy::Always
    );
    assert_eq!(
        SchemaAttachPolicy::Inherit.resolve(SchemaAttachPolicy::Never),
        SchemaAttachPolicy::Never
    );
}

#[test]
fn schema_attach_policy_resolve_override() {
    assert_eq!(
        SchemaAttachPolicy::Always.resolve(SchemaAttachPolicy::Never),
        SchemaAttachPolicy::Always
    );
    assert_eq!(
        SchemaAttachPolicy::Never.resolve(SchemaAttachPolicy::Always),
        SchemaAttachPolicy::Never
    );
}

#[test]
fn schema_attach_policy_clone() {
    let policy = SchemaAttachPolicy::Always;
    let cloned = policy;
    assert_eq!(policy, cloned);
}

#[test]
fn schema_attach_policy_copy() {
    let policy = SchemaAttachPolicy::Never;
    let copied = policy;
    assert_eq!(policy, copied);
}

#[test]
fn schema_attach_policy_debug() {
    let policy = SchemaAttachPolicy::Inherit;
    let debug = format!("{:?}", policy);
    assert!(debug.contains("Inherit"));
}

#[test]
fn schema_attach_policy_eq() {
    assert_eq!(SchemaAttachPolicy::Always, SchemaAttachPolicy::Always);
    assert_eq!(SchemaAttachPolicy::Never, SchemaAttachPolicy::Never);
    assert_eq!(SchemaAttachPolicy::Inherit, SchemaAttachPolicy::Inherit);
    assert_ne!(SchemaAttachPolicy::Always, SchemaAttachPolicy::Never);
}

#[test]
fn bfbs_contains_aorta_header_true() {
    // Real BFBS binaries store field names as null-terminated strings.
    let bfbs = b"table MyTable { aorta_header\0: AortaHeader; }";
    assert!(bfbs_contains_aorta_header(bfbs));
}

#[test]
fn bfbs_contains_aorta_header_false() {
    let bfbs = b"table MyTable { data: [ubyte]; }";
    assert!(!bfbs_contains_aorta_header(bfbs));
}

#[test]
fn bfbs_contains_aorta_header_empty() {
    let bfbs = b"";
    assert!(!bfbs_contains_aorta_header(bfbs));
}

#[test]
fn bfbs_contains_aorta_header_partial_match() {
    // The function now uses a null-terminated needle ("aorta_header\0") so that
    // substrings like "my_aorta_header_field" do NOT produce false positives.
    let bfbs = b"my_aorta_header_field: int;";
    assert!(!bfbs_contains_aorta_header(bfbs));

    // An exact null-terminated occurrence still matches.
    let bfbs_with_nul = b"my_field\0aorta_header\0other_field";
    assert!(bfbs_contains_aorta_header(bfbs_with_nul));
}

#[test]
fn schema_meta_trait_default_impl() {
    struct TestSchema;
    impl SchemaMeta for TestSchema {
        fn schema_bfbs() -> &'static [u8] {
            b"test bfbs data"
        }
    }

    let hash = TestSchema::schema_hash();
    let expected_hash = SchemaHash::from_bfbs_bytes(b"test bfbs data");
    assert_eq!(hash.as_bytes(), expected_hash.as_bytes());

    // Default fixed_size should be None
    assert_eq!(TestSchema::fixed_size(), None);
}

#[test]
fn schema_meta_trait_with_fixed_size() {
    struct FixedSizeSchema;
    impl SchemaMeta for FixedSizeSchema {
        fn schema_bfbs() -> &'static [u8] {
            b"fixed size schema"
        }

        fn fixed_size() -> Option<usize> {
            Some(64)
        }
    }

    assert_eq!(FixedSizeSchema::fixed_size(), Some(64));
}

#[test]
fn schema_hash_edge_cases() {
    // Empty data
    let hash_empty = SchemaHash::from_bfbs_bytes(b"");
    assert_eq!(hash_empty.as_bytes().len(), 32);

    // Large data
    let large_data = vec![0u8; 1024 * 1024];
    let hash_large = SchemaHash::from_bfbs_bytes(&large_data);
    assert_eq!(hash_large.as_bytes().len(), 32);

    // Binary data
    let binary_data: Vec<u8> = (0..256).map(|i| i as u8).collect();
    let hash_binary = SchemaHash::from_bfbs_bytes(&binary_data);
    assert_eq!(hash_binary.as_bytes().len(), 32);
}

// Suppress unused variable warnings for test data
#[allow(unused_variables)]
fn _suppress_warnings() {
    let _ = SchemaHash::from_bytes([0u8; 32]);
}
