#[allow(unused_imports, dead_code, non_snake_case)]
mod writer_cmd_generated;

#[test]
fn python_bulk_writer_is_readable() {
    let data = std::fs::read(std::env::var("WRITER_BUFFER").unwrap()).unwrap();
    let root = writer_cmd_generated::fx_write::root_as_low_cmd(&data).unwrap();
    assert_eq!(root.stamp(), 123);
    let motors = root.motor_cmd().unwrap();
    assert_eq!(motors.len(), 42);
    for (i, motor) in motors.iter().enumerate() {
        assert_eq!(motor.mode(), i as u8);
        assert_eq!(motor.q(), i as f32 + 0.125);
        assert_eq!(motor.dq(), i as f32 + 0.25);
        assert_eq!(motor.tau(), i as f32 + 0.5);
        assert_eq!(motor.kp(), i as f32 + 0.75);
        assert_eq!(motor.kd(), i as f32 + 0.875);
    }
    let commands = root.enums().unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands.get(0).mode().0, 127);
    assert_eq!(commands.get(0).value().to_bits(), (-0.0_f32).to_bits());
    assert_eq!(commands.get(0).counter(), u64::MAX);
    assert_eq!(commands.get(1).mode().0, -128);
    assert_eq!(commands.get(1).value(), f32::INFINITY);
    assert_eq!(commands.get(1).counter(), 0);
    let scalars = root.scalars().unwrap();
    assert_eq!(scalars.len(), 1);
    let scalar = scalars.get(0);
    assert!(scalar.enabled());
    assert_eq!(scalar.signed8(), i8::MIN);
    assert_eq!(scalar.unsigned8(), u8::MAX);
    assert_eq!(scalar.signed16(), i16::MIN);
    assert_eq!(scalar.unsigned16(), u16::MAX);
    assert_eq!(scalar.signed32(), i32::MIN);
    assert_eq!(scalar.unsigned32(), u32::MAX);
    assert_eq!(scalar.signed64(), i64::MIN);
    assert_eq!(scalar.unsigned64(), u64::MAX);
    assert_eq!(scalar.real32(), 0.1_f32);
    assert_eq!(scalar.real64(), -0.125);
}
