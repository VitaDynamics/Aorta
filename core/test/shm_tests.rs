use super::cleanup_orphaned_segments;

#[test]
fn cleanup_shm_does_not_panic() {
    cleanup_orphaned_segments();
}
