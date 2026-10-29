use super::*;
use crate::schema::SchemaHash;

#[test]
fn key_expr_builder_new_valid_group() {
    let builder = KeyExprBuilder::new("my_group");
    assert!(builder.is_ok());
    assert_eq!(builder.unwrap().group(), "my_group");
}

#[test]
fn key_expr_builder_new_empty_group() {
    let builder = KeyExprBuilder::new("");
    assert!(builder.is_err());
}

#[test]
fn key_expr_builder_new_whitespace_group() {
    let builder = KeyExprBuilder::new("   ");
    assert!(builder.is_err());
}

#[test]
fn key_expr_builder_new_multi_segment_group() {
    let builder = KeyExprBuilder::new("group/subgroup");
    assert!(builder.is_err());
}

#[test]
fn key_expr_builder_topic_key_strips_outer_slashes() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key = builder.topic_key_pub("/camera/image/").expect("key");
    let key_str: String = key.into();
    assert_eq!(key_str, "aorta/default/pub/camera/image");
}

#[test]
fn key_expr_builder_topic_key_rejects_empty_middle_segment() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    // Post-relaxation: `//` is a hard error (was silent collapse).
    assert!(builder.topic_key_pub("/camera//image").is_err());
}

#[test]
fn key_expr_builder_service_key_strips_outer_slashes() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key = builder.service_key_server("/nav/get_plan/").expect("key");
    let key_str: String = key.into();
    assert_eq!(key_str, "aorta/default/srv/nav/get_plan");
}

#[test]
fn key_expr_builder_param_key_normalizes_both_paths() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key = builder
        .param_key_owner("/my_node/", "/config/param/")
        .expect("key");
    let key_str: String = key.into();
    assert_eq!(key_str, "aorta/default/prm/my_node/config/param");
}

#[test]
fn key_expr_builder_schema_key_formats_correctly() {
    let mut bytes = [0u8; 32];
    bytes[0] = 0x12;
    bytes[31] = 0xEF;
    let hash = SchemaHash::from_bytes(bytes);

    let key = KeyExprBuilder::schema_key(&hash).expect("key");
    let key_str: String = key.into();
    assert_eq!(
        key_str,
        "aorta/sys/schema/12000000000000000000000000000000000000000000000000000000000000ef"
    );
}

#[test]
fn key_expr_builder_schema_by_key_strips_outer_slashes() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key = builder.schema_by_key("/pub/camera/image/").expect("key");
    let key_str: String = key.into();
    assert_eq!(key_str, "aorta/default/sys/schema_by_key/pub/camera/image");
}

#[test]
fn key_expr_builder_key_suffix_for_topic() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder
        .key_suffix_for_topic("/camera/image")
        .expect("suffix");
    assert_eq!(suffix, "pub/camera/image");
}

#[test]
fn key_expr_builder_key_suffix_for_service() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder.key_suffix_for_service("/nav/plan").expect("suffix");
    assert_eq!(suffix, "srv/nav/plan");
}

#[test]
fn key_expr_builder_key_suffix_for_param() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder
        .key_suffix_for_param("/my_node", "/max_vel")
        .expect("suffix");
    assert_eq!(suffix, "prm/my_node/max_vel");
}

#[test]
fn key_expr_builder_key_suffix_from_full_key_matching() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder.key_suffix_from_full_key("aorta/default/pub/camera/image");
    assert_eq!(suffix, Some("pub/camera/image".to_string()));
}

#[test]
fn key_expr_builder_key_suffix_from_full_key_different_group() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder.key_suffix_from_full_key("aorta/other_group/pub/camera/image");
    assert_eq!(suffix, None);
}

#[test]
fn key_expr_builder_key_suffix_from_full_key_no_prefix() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let suffix = builder.key_suffix_from_full_key("camera/image");
    assert_eq!(suffix, None);
}

#[test]
fn normalize_path_removes_leading_slash() {
    let path = normalize_path("/camera/image").expect("normalize");
    assert_eq!(path, "camera/image");
}

#[test]
fn normalize_path_removes_trailing_slash() {
    let path = normalize_path("camera/image/").expect("normalize");
    assert_eq!(path, "camera/image");
}

#[test]
fn normalize_path_rejects_empty_middle_segments() {
    // Replaces the old "collapses //" test — middle empties are now an
    // error, surfacing what used to be silently-fixed typos.
    assert!(normalize_path("camera//image").is_err());
    assert!(normalize_path("camera//image///raw").is_err());
}

#[test]
fn normalize_path_rejects_dot_segment() {
    let result = normalize_path("camera/./image");
    assert!(result.is_err());
}

#[test]
fn normalize_path_rejects_dotdot_segment() {
    let result = normalize_path("camera/../image");
    assert!(result.is_err());
}

#[test]
fn normalize_path_rejects_hash() {
    let result = normalize_path("camera#image");
    assert!(result.is_err());
}

#[test]
fn normalize_path_rejects_dollar() {
    let result = normalize_path("camera$image");
    assert!(result.is_err());
}

#[test]
fn normalize_path_rejects_question() {
    let result = normalize_path("camera?image");
    assert!(result.is_err());
}

#[test]
fn normalize_path_empty() {
    let result = normalize_path("");
    assert!(result.is_err());
}

#[test]
fn normalize_path_whitespace_only() {
    let result = normalize_path("   ");
    assert!(result.is_err());
}

#[test]
fn normalize_path_trims_whitespace() {
    let path = normalize_path("  camera/image  ").expect("normalize");
    assert_eq!(path, "camera/image");
}

#[test]
fn normalize_path_single_segment() {
    let path = normalize_path("camera").expect("normalize");
    assert_eq!(path, "camera");
}

#[test]
fn normalize_path_multiple_segments() {
    let path = normalize_path("a/b/c/d/e").expect("normalize");
    assert_eq!(path, "a/b/c/d/e");
}

#[test]
fn key_expr_builder_clone() {
    let builder1 = KeyExprBuilder::new("default").expect("builder");
    let builder2 = builder1.clone();

    assert_eq!(builder1.group(), builder2.group());

    // Both should work independently
    let key1 = builder1.topic_key_pub("/test").expect("key1");
    let key2 = builder2.topic_key_pub("/test").expect("key2");

    let s1: String = key1.into();
    let s2: String = key2.into();
    assert_eq!(s1, s2);
}

#[test]
fn key_expr_builder_debug() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let debug = format!("{:?}", builder);
    assert!(debug.contains("KeyExprBuilder"));
    assert!(debug.contains("default"));
}

#[test]
fn key_expr_builder_topic_key_empty() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let result = builder.topic_key_pub("");
    assert!(result.is_err());
}

#[test]
fn key_expr_builder_service_key_empty() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let result = builder.service_key_server("");
    assert!(result.is_err());
}

#[test]
fn key_expr_builder_param_key_empty_node() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let result = builder.param_key_owner("", "/param");
    assert!(result.is_err());
}

#[test]
fn key_expr_builder_param_key_empty_param() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let result = builder.param_key_owner("/node", "");
    assert!(result.is_err());
}

#[test]
fn key_expr_builder_schema_by_key_empty() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let result = builder.schema_by_key("");
    assert!(result.is_err());
}

#[test]
fn key_expr_builder_complex_path() {
    let builder = KeyExprBuilder::new("fleet_1").expect("builder");
    let key = builder
        .topic_key_pub("/robot/sensors/camera/front_left/image_raw")
        .expect("key");
    let key_str: String = key.into();
    assert_eq!(
        key_str,
        "aorta/fleet_1/pub/robot/sensors/camera/front_left/image_raw"
    );
}

#[test]
fn key_expr_builder_special_chars_in_segment() {
    let builder = KeyExprBuilder::new("default").expect("builder");

    // Underscore is allowed
    let key = builder.topic_key_pub("/camera_1/image_raw").expect("key");
    let key_str: String = key.into();
    assert!(key_str.contains("camera_1"));
    assert!(key_str.contains("image_raw"));
}

#[test]
fn key_expr_builder_leading_digit_segment_now_accepted() {
    // Pre-relaxation this was rejected (segments had to start with
    // `[a-z]`); post-relaxation any XID_Continue char is allowed at the
    // start, including digits.
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder
        .topic_key_pub("/camera/0/image")
        .expect("key")
        .into();
    assert_eq!(key, "aorta/default/pub/camera/0/image");
}

#[test]
fn key_expr_builder_hyphen_segment_now_accepted() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let key: String = builder.topic_key_pub("zenoh-rs-pub").expect("key").into();
    assert_eq!(key, "aorta/default/pub/zenoh-rs-pub");
}

#[test]
fn key_expr_builder_deeply_nested_path() {
    let builder = KeyExprBuilder::new("default").expect("builder");
    let deep_path = "/a/b/c/d/e/f/g/h/i/j";
    let key = builder.topic_key_pub(deep_path).expect("key");
    let key_str: String = key.into();
    assert!(key_str.contains("a/b/c/d/e/f/g/h/i/j"));
}

// Suppress unused variable warnings for test data
#[allow(unused_variables)]
fn _suppress_warnings() {
    let _ = KeyExprBuilder::new("test");
}
