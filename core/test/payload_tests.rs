use super::*;

#[test]
fn owned_payload_new_rejects_out_of_bounds_region() {
    let data = bytes::Bytes::from_static(&[1, 2, 3, 4]);
    let err = OwnedPayload::new(data, 2, 3).expect_err("must fail");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
    assert!(err.to_string().contains("out of bounds"));
}

#[test]
fn owned_payload_new_accepts_valid_region() {
    let data = bytes::Bytes::from_static(&[10, 20, 30, 40, 50]);
    let payload = OwnedPayload::new(data.clone(), 1, 3).expect("must succeed");
    assert_eq!(payload.offset(), 1);
    assert_eq!(payload.len(), 3);
    assert_eq!(payload.payload(), &[20, 30, 40]);
    assert_eq!(payload.data(), &data);
}

#[test]
fn owned_payload_from_bytes_wraps_full_buffer() {
    let data = bytes::Bytes::from_static(&[9, 8, 7]);
    let payload = OwnedPayload::from_bytes(data);
    assert_eq!(payload.offset(), 0);
    assert_eq!(payload.len(), 3);
    assert_eq!(payload.payload(), &[9, 8, 7]);
}

#[test]
fn owned_payload_into_bytes_returns_sliced_view() {
    let data = bytes::Bytes::from_static(&[0, 1, 2, 3, 4, 5]);
    let payload = OwnedPayload::new(data, 2, 2).expect("must succeed");
    let bytes = payload.into_bytes();
    assert_eq!(bytes.as_ref(), &[2, 3]);
}

#[test]
fn shm_payload_new_rejects_out_of_bounds_region() {
    let err = ShmPayload::new(7, 16, 12, 8).expect_err("must fail");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
    assert!(err.to_string().contains("out of bounds"));
}

#[test]
fn shm_payload_new_accepts_valid_region() {
    let payload = ShmPayload::new(42, 1024, 100, 256).expect("must succeed");
    assert_eq!(payload.handle(), 42);
    assert_eq!(payload.capacity(), 1024);
    assert_eq!(payload.offset(), 100);
    assert_eq!(payload.len(), 256);
}

#[test]
fn aorta_payload_len_and_empty_delegate_to_inner() {
    let owned = AortaPayload::Owned(OwnedPayload::from_bytes(bytes::Bytes::from_static(&[1, 2])));
    assert_eq!(owned.len(), 2);
    assert!(!owned.is_empty());

    let shm = AortaPayload::Shm(ShmPayload::new(1, 128, 16, 0).expect("must succeed"));
    assert_eq!(shm.len(), 0);
    assert!(shm.is_empty());
}
