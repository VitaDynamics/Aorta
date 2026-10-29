pub fn cleanup_orphaned_segments() {
    // Best-effort cleanup for leftover SHM segments on process shutdown.
    zenoh::shm::cleanup_orphaned_shm_segments();
}

#[cfg(test)]
#[path = "../test/shm_tests.rs"]
mod tests;
