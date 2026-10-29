/// Generates a canonical lowercase hyphenated UUIDv7 string using the current
/// Aorta Unix time and the core random source. This API is business-agnostic
/// and does not create event metadata.
///
/// The embedded 48-bit timestamp comes from [`crate::time::unix_time_ns`],
/// which returns `0` whenever the process time source resolves to a
/// non-positive value (e.g. `TimeSource::Simulation` selected before
/// `set_simulation_time_ns` is called). In that case this still returns a
/// well-formed UUIDv7, but its timestamp field is zeroed and it will *not*
/// sort/correlate by generation time against UUIDs minted under a live clock.
/// Callers relying on time-ordering (e.g. `user_interaction_id` /
/// trace-span correlation) must ensure the process time source is seeded
/// before generating IDs.
pub fn new_uuid_v7() -> String {
    let unix_ms = crate::time::unix_time_ns() / 1_000_000;
    let mut bytes = *::uuid::Uuid::new_v4().as_bytes();

    bytes[0] = ((unix_ms >> 40) & 0xff) as u8;
    bytes[1] = ((unix_ms >> 32) & 0xff) as u8;
    bytes[2] = ((unix_ms >> 24) & 0xff) as u8;
    bytes[3] = ((unix_ms >> 16) & 0xff) as u8;
    bytes[4] = ((unix_ms >> 8) & 0xff) as u8;
    bytes[5] = (unix_ms & 0xff) as u8;
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    ::uuid::Uuid::from_bytes(bytes).hyphenated().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_distinct_rfc_9562_uuid_v7_values() {
        let first = new_uuid_v7();
        let second = new_uuid_v7();

        assert_ne!(first, second);
        let parsed = ::uuid::Uuid::parse_str(&first).expect("valid UUID");
        assert_eq!(first, parsed.hyphenated().to_string());
        assert_eq!(parsed.get_version_num(), 7);
        assert_eq!(parsed.get_variant(), ::uuid::Variant::RFC4122);
    }
}
