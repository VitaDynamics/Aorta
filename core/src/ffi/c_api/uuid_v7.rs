use super::*;
use std::os::raw::c_char;

/// Generates one UUIDv7 into a caller-owned UTF-8 buffer.
///
/// # Safety
///
/// `out_text` must point to writable storage for `out_size` bytes. The buffer
/// must hold 37 bytes: 36 canonical UUID characters plus the trailing NUL.
#[no_mangle]
pub unsafe extern "C" fn aorta_uuid_v7(out_text: *mut c_char, out_size: usize) -> aorta_result_t {
    ffi_guard(|| {
        if out_text.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_text is null");
        }

        let text = crate::uuid_v7::new_uuid_v7();
        let required_size = text.len() + 1;
        if out_size < required_size {
            return aorta_err(
                AORTA_ERR_BUFFER_TOO_SMALL,
                "UUIDv7 output buffer must hold 37 bytes",
            );
        }
        // SAFETY: `out_text` was null-checked and the size check guarantees
        // room for all UTF-8 bytes plus the trailing NUL.
        unsafe {
            ptr::copy_nonoverlapping(text.as_ptr().cast::<c_char>(), out_text, text.len());
            *out_text.add(text.len()) = 0;
        }
        AORTA_OK
    })
}
