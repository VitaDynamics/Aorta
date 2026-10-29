#[cfg(test)]
const TEST_ALLOW_ENV: &str = "AORTA_TEST_ALLOW_ENV";

#[inline]
pub(crate) fn reads_enabled() -> bool {
    #[cfg(test)]
    {
        std::env::var_os(TEST_ALLOW_ENV).is_some()
    }

    #[cfg(not(test))]
    {
        true
    }
}

pub(crate) fn var(key: &str) -> Option<String> {
    if !reads_enabled() {
        return None;
    }
    std::env::var(key).ok()
}

pub(crate) fn var_os(key: &str) -> Option<std::ffi::OsString> {
    if !reads_enabled() {
        return None;
    }
    std::env::var_os(key)
}
