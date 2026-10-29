// Logging FFI surface.
//
// Extracted from c_api/mod.rs.

use super::*;

#[no_mangle]
pub extern "C" fn aorta_log_init_from_env() -> aorta_result_t {
    ffi_guard(|| match logging::init_from_env_once() {
        Ok(_) => AORTA_OK,
        Err(err) => map_aorta_error(err),
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. The caller retains ownership of all input buffers;
/// this function does not free or take ownership of pointer arguments.
#[no_mangle]
pub unsafe extern "C" fn aorta_log_configure(config: *const aorta_log_config_t) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the null-checked
    // `&*config` deref plus the `cstr_to_string_optional` calls on the
    // caller-supplied C-string fields, all under the FFI pointer contract.
    ffi_guard(|| unsafe {
        if config.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "config is null");
        }

        let config = &*config;
        let mut resolved = LogConfig::default();
        if let Some(raw) = cstr_to_string_optional(config.filter) {
            if !raw.trim().is_empty() {
                resolved.filter = raw;
            }
        }
        resolved.file_path = cstr_to_string_optional(config.file_path)
            .map(|raw| raw.trim().to_string())
            .filter(|raw| !raw.is_empty())
            .map(std::path::PathBuf::from);
        resolved.format = log_format_from_ffi(config.format);
        resolved.stderr_enabled = config.stderr_enabled != 0;

        match logging::configure_once(resolved) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn aorta_log_flush() -> aorta_result_t {
    ffi_guard(|| {
        logging::flush();
        AORTA_OK
    })
}

fn ensure_logging_initialized() -> AortaResult<()> {
    if !logging::is_initialized() {
        match logging::init_from_env_once() {
            Ok(_) => {}
            Err(AortaError::Busy(_)) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. The caller retains ownership of all input buffers;
/// this function does not free or take ownership of pointer arguments.
#[no_mangle]
pub unsafe extern "C" fn aorta_log_write(
    level: aorta_log_level_t,
    scope: *const c_char,
    message: *const c_char,
) -> aorta_result_t {
    // No ffi_guard here: aorta_log_write_at is itself wrapped, and double-
    // wrapping just adds an unneeded catch_unwind frame on the log hot path.
    // SAFETY: trampoline forwarding `scope` and `message` to
    // `aorta_log_write_at` under that fn's matching `# Safety` contract.
    unsafe { aorta_log_write_at(level, scope, ptr::null(), 0, message) }
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. The caller retains ownership of all input buffers;
/// this function does not free or take ownership of pointer arguments.
#[no_mangle]
pub unsafe extern "C" fn aorta_log_write_at(
    level: aorta_log_level_t,
    scope: *const c_char,
    file: *const c_char,
    line: u32,
    message: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // `cstr_to_string*` calls on `scope`, `file`, `message` — all C-string
    // pointers under the FFI caller's NUL-termination + UTF-8 contract.
    ffi_guard(|| unsafe {
        let scope = match cstr_to_string(scope, "scope") {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };
        let file = cstr_to_string_optional(file);
        let message = match cstr_to_string(message, "message") {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };

        if let Err(err) = ensure_logging_initialized() {
            return map_aorta_error(err);
        }

        let line = if line == 0 { None } else { Some(line) };
        emit_ffi_log(level, &scope, file.as_deref(), line, &message);
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. The caller retains ownership of all input buffers;
/// this function does not free or take ownership of pointer arguments.
#[no_mangle]
pub unsafe extern "C" fn aorta_log_set_context(
    node_name: *const c_char,
    group: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // `cstr_to_string` calls on `node_name` and `group` C-string pointers,
    // both under the FFI caller's NUL-termination + UTF-8 contract.
    ffi_guard(|| unsafe {
        let node_name = match cstr_to_string(node_name, "node_name") {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };
        let group = match cstr_to_string(group, "group") {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };

        if logging::auto_init_enabled() {
            if let Err(err) = ensure_logging_initialized() {
                return map_aorta_error(err);
            }
        }

        match logging::set_default_context(&node_name, &group) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
        }
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. The caller retains ownership of all input buffers;
/// this function does not free or take ownership of pointer arguments.
#[no_mangle]
pub unsafe extern "C" fn aorta_log_enabled(level: aorta_log_level_t, scope: *const c_char) -> u8 {
    // Fallback 0: on panic, signal "not enabled" so the caller skips the log
    // call site rather than maybe-emitting a half-formed line.
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `cstr_to_string`
    // on the caller-supplied `scope` C-string under the NUL-termination
    // contract.
    ffi_guard_value(0, || unsafe {
        let scope = match cstr_to_string(scope, "scope") {
            Ok(value) => value,
            Err(err) => {
                let _ = map_aorta_error(err);
                return 0;
            }
        };
        let (target, _) = ffi_log_target_from_scope(&scope);
        if logging::log_enabled(log_level_from_ffi(level), ffi_log_target_name(target)) {
            1
        } else {
            0
        }
    })
}
