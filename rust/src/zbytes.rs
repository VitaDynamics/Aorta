use aorta_core::ZBytes;

/// Provides a contiguous byte slice view for a `ZBytes` payload.
///
/// Fast path: if payload has a single slice, borrow it directly (zero-copy).
/// Fallback: if payload is multi-slice, materialize a contiguous buffer.
pub fn with_contiguous_bytes<R, F>(payload: &ZBytes, f: F) -> R
where
    F: FnOnce(&[u8]) -> R,
{
    let mut slices = payload.slices();
    let Some(first) = slices.next() else {
        return f(&[]);
    };
    if slices.next().is_none() {
        return f(first);
    }
    let bytes = payload.to_bytes();
    f(bytes.as_ref())
}

#[cfg(test)]
mod tests {
    use super::with_contiguous_bytes;
    use aorta_core::ZBytes;

    #[test]
    fn with_contiguous_bytes_borrows_single_slice() {
        let payload = ZBytes::from(vec![1u8, 2, 3, 4]);
        let out = with_contiguous_bytes(&payload, |bytes| bytes.to_vec());
        assert_eq!(out, vec![1u8, 2, 3, 4]);
    }

    #[test]
    fn with_contiguous_bytes_merges_multi_slice() {
        let mut writer = ZBytes::writer();
        writer.append(ZBytes::from(vec![1u8, 2]));
        writer.append(ZBytes::from(vec![3u8, 4, 5]));
        let payload = writer.finish();

        let out = with_contiguous_bytes(&payload, |bytes| bytes.to_vec());
        assert_eq!(out, vec![1u8, 2, 3, 4, 5]);
    }

    #[test]
    fn with_contiguous_bytes_handles_empty_payload() {
        let payload = ZBytes::new();
        let out = with_contiguous_bytes(&payload, |bytes| bytes.len());
        assert_eq!(out, 0);
    }
}
