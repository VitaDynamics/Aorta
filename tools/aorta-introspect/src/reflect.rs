use anyhow::{anyhow, Result};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr;

unsafe extern "C" {
    fn aorta_introspect_fb_to_json(
        bfbs_data: *const u8,
        bfbs_size: usize,
        payload_data: *const u8,
        payload_size: usize,
        root_type: *const c_char,
        root_type_size: usize,
        out_json: *mut *mut c_char,
    ) -> c_int;

    fn aorta_introspect_fb_to_json_display(
        bfbs_data: *const u8,
        bfbs_size: usize,
        payload_data: *const u8,
        payload_size: usize,
        root_type: *const c_char,
        root_type_size: usize,
        out_json: *mut *mut c_char,
    ) -> c_int;

    fn aorta_introspect_bfbs_to_schema_text(
        bfbs_data: *const u8,
        bfbs_size: usize,
        root_type: *const c_char,
        root_type_size: usize,
        out_text: *mut *mut c_char,
    ) -> c_int;

    fn aorta_introspect_free_string(str: *mut c_char);

    fn aorta_introspect_last_error() -> *const c_char;

    fn aorta_introspect_json_to_fb(
        bfbs_data: *const u8,
        bfbs_size: usize,
        json_data: *const c_char,
        json_size: usize,
        root_type: *const c_char,
        root_type_size: usize,
        out_fb: *mut *mut u8,
        out_fb_size: *mut usize,
    ) -> c_int;

    fn aorta_introspect_free_bytes(data: *mut u8);

    fn aorta_introspect_json_to_fb_last_error() -> *const c_char;
}

type FbToJsonFn = unsafe extern "C" fn(
    *const u8,
    usize,
    *const u8,
    usize,
    *const c_char,
    usize,
    *mut *mut c_char,
) -> c_int;

fn fb_to_json_impl(
    bfbs_data: &[u8],
    fb_data: &[u8],
    root_type: Option<&str>,
    ffi_fn: FbToJsonFn,
) -> Result<String> {
    if bfbs_data.is_empty() {
        return Err(anyhow!("bfbs schema is empty"));
    }

    let (root_type_ptr, root_type_size) = root_type_ptr(root_type);
    let mut out_json: *mut c_char = ptr::null_mut();
    let rc = unsafe {
        ffi_fn(
            bfbs_data.as_ptr(),
            bfbs_data.len(),
            fb_data.as_ptr(),
            fb_data.len(),
            root_type_ptr,
            root_type_size,
            &mut out_json,
        )
    };

    if rc != 0 {
        return Err(anyhow!(
            "reflection bridge failed: {}",
            ffi_error(aorta_introspect_last_error)
        ));
    }
    if out_json.is_null() {
        return Err(anyhow!("reflection bridge returned null json pointer"));
    }

    let json = unsafe { CStr::from_ptr(out_json).to_string_lossy().into_owned() };
    unsafe {
        aorta_introspect_free_string(out_json);
    }
    Ok(json)
}

pub fn fb_to_json(bfbs_data: &[u8], fb_data: &[u8], root_type: Option<&str>) -> Result<String> {
    fb_to_json_impl(bfbs_data, fb_data, root_type, aorta_introspect_fb_to_json)
}

/// Render a FlatBuffer as human-readable text with enum identifiers
/// emitted unquoted (e.g. `status: RUNNING`).
///
/// The output is **NOT valid JSON** — feed it only to UI/log surfaces,
/// never to a JSON parser. Callers that need machine-parseable output
/// should use [`fb_to_json`] instead.
///
/// All enum fields are unquoted: top-level, nested-struct, vector,
/// union type-tag, and enum fields inside union payloads.
pub fn fb_to_json_display(
    bfbs_data: &[u8],
    fb_data: &[u8],
    root_type: Option<&str>,
) -> Result<String> {
    fb_to_json_impl(
        bfbs_data,
        fb_data,
        root_type,
        aorta_introspect_fb_to_json_display,
    )
}

pub fn bfbs_to_schema_text(bfbs_data: &[u8], root_type: Option<&str>) -> Result<String> {
    if bfbs_data.is_empty() {
        return Err(anyhow!("bfbs schema is empty"));
    }

    let (root_type_ptr, root_type_size) = root_type_ptr(root_type);
    let mut out_text: *mut c_char = ptr::null_mut();
    let rc = unsafe {
        aorta_introspect_bfbs_to_schema_text(
            bfbs_data.as_ptr(),
            bfbs_data.len(),
            root_type_ptr,
            root_type_size,
            &mut out_text,
        )
    };

    if rc != 0 {
        return Err(anyhow!(
            "schema reflection bridge failed: {}",
            ffi_error(aorta_introspect_last_error)
        ));
    }
    if out_text.is_null() {
        return Err(anyhow!(
            "schema reflection bridge returned null text pointer"
        ));
    }

    let text = unsafe { CStr::from_ptr(out_text).to_string_lossy().into_owned() };
    unsafe {
        aorta_introspect_free_string(out_text);
    }
    Ok(text)
}

pub fn json_to_fb(bfbs_data: &[u8], json_text: &str, root_type: Option<&str>) -> Result<Vec<u8>> {
    if bfbs_data.is_empty() {
        return Err(anyhow!("bfbs schema is empty"));
    }

    let (root_type_ptr, root_type_size) = root_type_ptr(root_type);
    let mut out_fb: *mut u8 = ptr::null_mut();
    let mut out_fb_size: usize = 0;
    let rc = unsafe {
        aorta_introspect_json_to_fb(
            bfbs_data.as_ptr(),
            bfbs_data.len(),
            json_text.as_ptr() as *const c_char,
            json_text.len(),
            root_type_ptr,
            root_type_size,
            &mut out_fb,
            &mut out_fb_size,
        )
    };
    if rc != 0 {
        return Err(anyhow!(
            "json_to_fb bridge failed: {}",
            ffi_error(aorta_introspect_json_to_fb_last_error)
        ));
    }
    if out_fb.is_null() && out_fb_size > 0 {
        return Err(anyhow!("json_to_fb bridge returned null output buffer"));
    }

    let out = if out_fb_size == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(out_fb, out_fb_size).to_vec() }
    };
    unsafe {
        aorta_introspect_free_bytes(out_fb);
    }
    Ok(out)
}

fn root_type_ptr(root_type: Option<&str>) -> (*const c_char, usize) {
    if let Some(value) = root_type {
        (value.as_ptr() as *const c_char, value.len())
    } else {
        (ptr::null(), 0)
    }
}

fn ffi_error(getter: unsafe extern "C" fn() -> *const c_char) -> String {
    unsafe {
        let ptr = getter();
        if ptr.is_null() {
            "unknown reflection error".to_string()
        } else {
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{bfbs_to_schema_text, fb_to_json, fb_to_json_display, json_to_fb};

    #[test]
    fn fb_to_json_rejects_invalid_bfbs() {
        let bfbs = [1u8];
        let payload = [0x12u8, 0x34u8];
        let err = fb_to_json(&bfbs, &payload, None).expect_err("invalid bfbs should fail");
        assert!(err.to_string().contains("reflection bridge failed"));
    }

    #[test]
    fn fb_to_json_display_rejects_invalid_bfbs() {
        let bfbs = [1u8];
        let payload = [0x12u8, 0x34u8];
        let err = fb_to_json_display(&bfbs, &payload, None).expect_err("invalid bfbs should fail");
        assert!(err.to_string().contains("reflection bridge failed"));
    }

    #[test]
    fn bfbs_to_schema_text_rejects_invalid_bfbs() {
        let bfbs = [1u8];
        let err = bfbs_to_schema_text(&bfbs, None).expect_err("invalid bfbs should fail");
        assert!(err.to_string().contains("schema reflection bridge failed"));
    }

    #[test]
    fn json_to_fb_rejects_invalid_bfbs() {
        let bfbs = [1u8, 2u8];
        let err = json_to_fb(&bfbs, "{\"a\":1}", None).expect_err("invalid bfbs should fail");
        assert!(err.to_string().contains("json_to_fb bridge failed"));
    }

    // ----------------------------------------------------------------
    // Happy-path tests for fb_to_json_display.
    //
    // These tests compile a small fixture schema with `flatc` at test
    // time. If `flatc` is not on PATH (and FLATC env var is unset),
    // the helper returns None and the test is skipped, so they never
    // block CI environments that lack the toolchain.
    // ----------------------------------------------------------------

    // A second fixture that exercises (id: N) explicit field ordering and
    // non-first-variant enum defaults — the exact conditions that triggered
    // the earlier garbled-value / SIGSEGV failures.
    //
    // Vtable slots are assigned by (id: N): slot 0 = runtime_status, slot 1 =
    // degraded_reason, slot 2 = family. Declaration order is the reverse
    // (family first), so any reader that uses declaration index instead of
    // the id-assigned offset will read wrong vtable slots. Ids are contiguous
    // from 0 as required by the FlatBuffers schema rules (gapped ids are
    // rejected by flatc).
    const FIXTURE_FBS_DENSE: &str = r#"
namespace aorta.display_test;

enum RuntimeStatus : byte {
  Active = 0,
  Unavailable = 1,
  Degraded = 2,
}

table DenseCapability {
  family: string (id: 2);
  runtime_status: RuntimeStatus = Unavailable (id: 0);
  degraded_reason: string (id: 1);
}

root_type DenseCapability;
"#;

    const FIXTURE_FBS: &str = r#"
namespace aorta.display_test;

enum Status : byte {
  Idle = 0,
  Running = 1,
  Done = 2,
}

enum LogLevel : byte {
  Info = 0,
  Warn = 1,
  Error = 2,
}

table StartCmd {
  delay_ms:int;
  level:LogLevel;
}

table StopCmd {
  reason:string;
}

union Command {
  StartCmd,
  StopCmd,
}

table Inner {
  status:Status;
  message:string;
}

table Sample {
  status:Status;
  name:string;
  inner:Inner;
  levels:[LogLevel];
  command:Command;
}

root_type Sample;
"#;

    // Fixture for: bit_flags enum, nested union (union-in-union-payload),
    // vector-of-union, fixed-size array (in a struct), and empty-vector.
    //
    // Note: fixed-size arrays ([T:N]) are only valid inside FlatBuffers
    // structs; Triplet wraps the array so it can appear as a table field.
    const FIXTURE_FBS_V2: &str = r#"
namespace aorta.display_test;

enum Permission : uint8 (bit_flags) {
  Read,
  Write,
  Exec,
}

enum LogLevel : byte {
  Info = 0,
  Warn = 1,
  Error = 2,
}

struct Triplet {
  vals: [int:3];
}

table StartCmd {
  delay_ms: int;
  level: LogLevel;
}

table StopCmd {
  reason: string;
}

union Command {
  StartCmd,
  StopCmd,
}

// A table that itself contains a union — used as a payload of Outer,
// giving us a nested union (Outer -> HolderCmd -> Command).
table HolderCmd {
  inner: Command;
  label: string;
}

union Outer {
  HolderCmd,
  StopCmd,
}

table Multi {
  perms: Permission;
  cmds: [Command];
  triplet: Triplet;
  tags: [LogLevel];
  outer: Outer;
}

root_type Multi;
"#;

    fn resolve_flatc() -> Option<std::path::PathBuf> {
        if let Ok(path) = std::env::var("FLATC") {
            let p = std::path::PathBuf::from(path);
            if p.is_file() {
                return Some(p);
            }
        }
        let candidates = ["flatc", "/opt/homebrew/bin/flatc", "/usr/local/bin/flatc"];
        for c in candidates {
            let out = std::process::Command::new(c).arg("--version").output();
            if let Ok(o) = out {
                if o.status.success() {
                    return Some(std::path::PathBuf::from(c));
                }
            }
        }
        None
    }

    fn build_bfbs_from(fbs_src: &str, stem: &str) -> Option<Vec<u8>> {
        let flatc = resolve_flatc()?;
        // Per-invocation directory: cargo/rust test runs tests in parallel
        // threads, and multiple tests sharing the same fixture stem
        // (e.g. `display_test_v2`) would otherwise race on the same
        // .fbs/.bfbs paths and intermittently observe an empty bfbs.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("aorta-display-test-{}-{}", std::process::id(), seq));
        std::fs::create_dir_all(&dir).ok()?;
        let fbs_path = dir.join(format!("{stem}.fbs"));
        std::fs::write(&fbs_path, fbs_src).ok()?;
        // `--bfbs-builtins` is required so built-in attributes (bit_flags,
        // key, required, etc.) survive into the bfbs and can be inspected via
        // `EnumDef::attributes.Lookup(...)`. Without it the bit_flags branch
        // in `DisplayPrinter::PrintScalar` never fires for bfbs-loaded enums.
        let status = std::process::Command::new(&flatc)
            .arg("--binary")
            .arg("--schema")
            .arg("--bfbs-builtins")
            .arg("-o")
            .arg(&dir)
            .arg(&fbs_path)
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }
        let bytes = std::fs::read(dir.join(format!("{stem}.bfbs"))).ok()?;
        // Some flatc builds (observed on the project's self-hosted CI
        // runners) silently emit an empty .bfbs file when the
        // `--bfbs-builtins` flag is unsupported by the installed
        // version: status is 0, the file exists, but its length is 0.
        // Returning Some(empty) here would propagate to `json_to_fb`
        // which then panics with "bfbs schema is empty", surfacing as
        // a flaky test failure. Treat an
        // empty bfbs the same as a missing flatc: skip the test.
        if bytes.is_empty() {
            return None;
        }
        Some(bytes)
    }

    fn build_fixture_bfbs() -> Option<Vec<u8>> {
        build_bfbs_from(FIXTURE_FBS, "display_test")
    }

    fn build_fixture_dense_bfbs() -> Option<Vec<u8>> {
        build_bfbs_from(FIXTURE_FBS_DENSE, "display_test_dense")
    }

    fn build_fixture_v2_bfbs() -> Option<Vec<u8>> {
        build_bfbs_from(FIXTURE_FBS_V2, "display_test_v2")
    }

    #[test]
    fn fb_to_json_display_unquotes_top_level_enum() {
        let Some(bfbs) = build_fixture_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        // Build a minimal payload: only Status and name set.
        let json = r#"{"status":"Running","name":"abc123"}"#;
        let fb = json_to_fb(&bfbs, json, Some("aorta.display_test.Sample"))
            .expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.Sample"))
            .expect("fb_to_json_display succeeds");

        // status (enum) must be unquoted, name (string) must keep quotes.
        assert!(
            display.contains("status: Running"),
            "expected unquoted enum, got: {display}"
        );
        assert!(
            display.contains("name: \"abc123\""),
            "expected quoted string, got: {display}"
        );
        // The original quoted form must NOT appear.
        assert!(
            !display.contains("\"Running\""),
            "enum should be unquoted, got: {display}"
        );
    }

    #[test]
    fn fb_to_json_display_unquotes_nested_and_vector_enums() {
        let Some(bfbs) = build_fixture_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        // Use only non-default enum values: flatbuffers omits default-valued
        // scalars from JSON output, so Status=Idle (0) and LogLevel=Info (0)
        // would never appear in the rendered text.
        let json = r#"{
            "status":"Done",
            "inner":{"status":"Running","message":"hi"},
            "levels":["Warn","Error"]
        }"#;
        let fb = json_to_fb(&bfbs, json, Some("aorta.display_test.Sample"))
            .expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.Sample"))
            .expect("fb_to_json_display succeeds");

        assert!(display.contains("status: Done"), "{display}");
        assert!(display.contains("status: Running"), "{display}");
        assert!(display.contains("message: \"hi\""), "{display}");
        // Vector of enums: each element unquoted.
        assert!(display.contains("Warn"), "{display}");
        assert!(display.contains("Error"), "{display}");
        assert!(!display.contains("\"Warn\""), "{display}");
        assert!(!display.contains("\"Error\""), "{display}");
    }

    #[test]
    fn fb_to_json_display_preserves_union_payload_structure() {
        // Regression test for the union-handling fix: the union payload
        // is an object whose concrete type is determined at runtime, so
        // the post-processing parser must copy it verbatim. Without the
        // fix, the JSON object structure was corrupted.
        let Some(bfbs) = build_fixture_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        let json = r#"{
            "status":"Running",
            "command_type":"StartCmd",
            "command":{"delay_ms":42,"level":"Warn"}
        }"#;
        let fb = json_to_fb(&bfbs, json, Some("aorta.display_test.Sample"))
            .expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.Sample"))
            .expect("fb_to_json_display succeeds");

        // Union type tag is a UTYPE enum -> unquoted.
        assert!(display.contains("command_type: StartCmd"), "{display}");
        // Union payload object must remain structurally intact: fields
        // delay_ms and level both present in the output.
        assert!(display.contains("delay_ms: 42"), "{display}");
        assert!(display.contains("level"), "{display}");
        // Closing brace of the union object must be emitted.
        let open = display.matches('{').count();
        let close = display.matches('}').count();
        assert_eq!(open, close, "unbalanced braces, got: {display}");
    }

    #[test]
    fn fb_to_json_display_unquotes_union_payload_enum() {
        // Core motivation for this task: enum fields *inside* a union payload
        // must be unquoted. The old GenerateText+post-processing approach could
        // not resolve the sibling *_type tag and left them quoted.
        let Some(bfbs) = build_fixture_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        let json = r#"{
            "status":"Running",
            "command_type":"StartCmd",
            "command":{"delay_ms":100,"level":"Warn"}
        }"#;
        let fb = json_to_fb(&bfbs, json, Some("aorta.display_test.Sample"))
            .expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.Sample"))
            .expect("fb_to_json_display succeeds");

        // The enum inside the union payload must be unquoted.
        assert!(
            display.contains("level: Warn"),
            "union payload enum should be unquoted, got: {display}"
        );
        assert!(
            !display.contains("\"Warn\""),
            "enum should not be quoted anywhere, got: {display}"
        );
        // Union type tag also unquoted.
        assert!(display.contains("command_type: StartCmd"), "{display}");
    }

    #[test]
    fn fb_to_json_display_handles_non_first_variant_default_and_explicit_ids() {
        // Regression coverage: schemas where enum fields carry
        // non-first-variant defaults and all fields use explicit (id: N)
        // attributes previously caused garbled values / SIGSEGV because the
        // old GenerateText path misread vtable slots.
        //
        // This test verifies that:
        // 1. Fields present in the binary are decoded with correct values.
        // 2. A field absent from the binary (== declared default = Unavailable)
        //    does not appear as "Active" (value 0) or any other wrong value.
        // 3. No crash or non-utf8 error occurs.
        let Some(bfbs) = build_fixture_dense_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        // Serialize a row where runtime_status is Degraded (non-default, stored)
        // and degraded_reason is set.
        let json_degraded =
            r#"{"family":"cpu","runtime_status":"Degraded","degraded_reason":"overtemp"}"#;
        let fb = json_to_fb(
            &bfbs,
            json_degraded,
            Some("aorta.display_test.DenseCapability"),
        )
        .expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.DenseCapability"))
            .expect("fb_to_json_display succeeds");

        assert!(display.contains("family: \"cpu\""), "{display}");
        assert!(display.contains("runtime_status: Degraded"), "{display}");
        assert!(!display.contains("\"Degraded\""), "{display}");
        assert!(
            display.contains("degraded_reason: \"overtemp\""),
            "{display}"
        );

        // Serialize a row where runtime_status equals the declared default
        // (Unavailable = 1); FlatBuffers omits it from the binary. The
        // display printer synthesizes the *declared* default
        // (Unavailable) rather than dropping the field — and must still never
        // misread the absent slot as Active (0), which was the original bug.
        let json_default = r#"{"family":"psi","runtime_status":"Unavailable"}"#;
        let fb2 = json_to_fb(
            &bfbs,
            json_default,
            Some("aorta.display_test.DenseCapability"),
        )
        .expect("json_to_fb succeeds");
        let display2 = fb_to_json_display(&bfbs, &fb2, Some("aorta.display_test.DenseCapability"))
            .expect("fb_to_json_display succeeds");

        assert!(display2.contains("family: \"psi\""), "{display2}");
        // Absent default scalar is now surfaced at its declared default.
        assert!(
            display2.contains("runtime_status: Unavailable"),
            "absent default scalar must render at its declared default: {display2}"
        );
        // Must not misread the absent field as Active (the zero/first variant).
        assert!(
            !display2.contains("Active"),
            "absent default field must not appear as Active: {display2}"
        );
    }

    #[test]
    fn fb_to_json_default_scalars_are_surfaced_in_both_paths() {
        // A `FlatBufferBuilder` without ForceDefaults elides
        // scalar fields equal to their schema default from the wire. Echo /
        // decode previously dropped them, so `error_code = 0`, `flag = false`
        // or an enum at its default read as "never written". Both the strict
        // JSON path (`fb_to_json`, output_default_scalars_in_json) and the
        // display path (`fb_to_json_display`, synthesized default) must now
        // surface such a field.
        let Some(bfbs) = build_fixture_dense_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        // runtime_status == Unavailable is the declared default → omitted on
        // the wire; degraded_reason (string, no default) stays absent.
        let json_default = r#"{"family":"psi","runtime_status":"Unavailable"}"#;
        let fb = json_to_fb(
            &bfbs,
            json_default,
            Some("aorta.display_test.DenseCapability"),
        )
        .expect("json_to_fb succeeds");

        let strict = fb_to_json(&bfbs, &fb, Some("aorta.display_test.DenseCapability"))
            .expect("fb_to_json succeeds");
        let strict_value: serde_json::Value =
            serde_json::from_str(&strict).expect("fb_to_json must return valid JSON");
        assert!(
            strict.contains("runtime_status") && strict.contains("Unavailable"),
            "strict JSON must surface the default scalar: {strict}"
        );
        assert_eq!(strict_value["runtime_status"], "Unavailable");

        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.DenseCapability"))
            .expect("fb_to_json_display succeeds");
        assert!(
            display.contains("runtime_status: Unavailable"),
            "display must surface the default scalar: {display}"
        );
        // A field with no default (string) still stays absent when unset.
        assert!(
            !display.contains("degraded_reason"),
            "absent no-default field must stay omitted: {display}"
        );
    }

    // ── New regression tests ─────────────────────────────────────────────────

    #[test]
    fn fb_to_json_display_bit_flags_combination() {
        // A combined bit_flags value (Read|Write = 3) must expand to
        // space-separated names, not fall through to a raw integer.
        let Some(bfbs) = build_fixture_v2_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        let json = r#"{"perms":3}"#; // Read=1 | Write=2
        let fb =
            json_to_fb(&bfbs, json, Some("aorta.display_test.Multi")).expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.Multi"))
            .expect("fb_to_json_display succeeds");

        assert!(
            display.contains("perms: Read Write"),
            "bit_flags combination must expand to named bits, got: {display}"
        );
        assert!(
            !display.contains("perms: 3"),
            "bit_flags must not be rendered as a raw integer, got: {display}"
        );
    }

    #[test]
    fn fb_to_json_display_nested_union() {
        // outer is Outer::HolderCmd; HolderCmd.inner is Command::StartCmd.
        // Verifies that enum values inside a doubly-nested union are unquoted.
        let Some(bfbs) = build_fixture_v2_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        let json = r#"{
            "outer_type": "HolderCmd",
            "outer": {
                "inner_type": "StartCmd",
                "inner": {"delay_ms": 10, "level": "Warn"},
                "label": "nested"
            }
        }"#;
        let fb =
            json_to_fb(&bfbs, json, Some("aorta.display_test.Multi")).expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.Multi"))
            .expect("fb_to_json_display succeeds");

        assert!(display.contains("outer_type: HolderCmd"), "{display}");
        assert!(display.contains("inner_type: StartCmd"), "{display}");
        // Enum inside the nested union payload must be unquoted.
        assert!(display.contains("level: Warn"), "{display}");
        assert!(!display.contains("\"Warn\""), "{display}");
        assert!(display.contains("label: \"nested\""), "{display}");
    }

    #[test]
    fn fb_to_json_display_vector_of_union() {
        // cmds is [Command] — a union vector.
        // Verifies that each element's payload enum is unquoted.
        let Some(bfbs) = build_fixture_v2_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        let json = r#"{
            "cmds_type": ["StartCmd", "StopCmd"],
            "cmds": [
                {"delay_ms": 5, "level": "Warn"},
                {"reason": "done"}
            ]
        }"#;
        let fb =
            json_to_fb(&bfbs, json, Some("aorta.display_test.Multi")).expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.Multi"))
            .expect("fb_to_json_display succeeds");

        // Enum inside the union vector element must be unquoted.
        assert!(display.contains("level: Warn"), "{display}");
        assert!(!display.contains("\"Warn\""), "{display}");
        assert!(display.contains("reason: \"done\""), "{display}");
        // Braces must balance.
        assert_eq!(
            display.matches('{').count(),
            display.matches('}').count(),
            "unbalanced braces: {display}"
        );
    }

    #[test]
    fn fb_to_json_display_fixed_array() {
        // triplet holds a Triplet struct whose `vals` field is [int:3] —
        // a fixed-size inline array. Verifies it renders as a bracket-
        // enclosed list of integers.
        let Some(bfbs) = build_fixture_v2_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        let json = r#"{"triplet": {"vals": [10, 20, 30]}}"#;
        let fb =
            json_to_fb(&bfbs, json, Some("aorta.display_test.Multi")).expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.Multi"))
            .expect("fb_to_json_display succeeds");

        assert!(display.contains("vals:"), "{display}");
        assert!(display.contains("10"), "{display}");
        assert!(display.contains("20"), "{display}");
        assert!(display.contains("30"), "{display}");
    }

    #[test]
    fn fb_to_json_display_empty_vector() {
        // tags is [LogLevel]; when the field is set to an empty vector the
        // output must render `[]`, not crash or silently omit the field.
        let Some(bfbs) = build_fixture_v2_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };
        let json = r#"{"tags": []}"#;
        let fb =
            json_to_fb(&bfbs, json, Some("aorta.display_test.Multi")).expect("json_to_fb succeeds");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.Multi"))
            .expect("fb_to_json_display succeeds");

        assert!(
            display.contains("tags: []"),
            "empty vector must render as []: {display}"
        );
    }

    // ── Sysmon-scale fixture ───────────────────────────────────────────────
    //
    // End-to-end coverage for the original failure cluster (garbled
    // values, non-utf8 errors, intermittent SIGSEGV on SysmonSnapshot-shaped
    // payloads). `DisplayPrinter` was expected to fix it by
    // construction (per-field `CheckField` + base-type dispatch), but the
    // existing `FIXTURE_FBS_DENSE` only covers 3 fields. This schema mirrors
    // `message/schemas/system/sysmon_snapshot.fbs`: large root table (~19
    // ids), several sub-tables with 25+ ids, vectors of sub-tables, mixed
    // scalar widths, explicit `(id: N)` on every field, and non-first-variant
    // enum defaults on the `runtime_status` field.
    //
    // `AortaHeader` is inlined here as a sibling table so the fixture stays
    // self-contained (the real schema includes it from
    // `schemas/system/aorta_base.fbs`).
    const FIXTURE_FBS_SYSMON: &str = r#"
namespace aorta.display_test.sysmon;

enum ConfiguredMode : ubyte {
  ON = 0,
  OFF = 1,
  AUTO = 2,
}

enum RuntimeStatus : ubyte {
  ACTIVE = 0,
  UNAVAILABLE = 1,
  DEGRADED = 2,
}

table AortaHeader {
  publish_stamp_ns: ulong  (id: 0);
  publisher_node:   string (id: 1);
  sequence:         ulong  (id: 2);
}

table MetricCapability {
  family:               string                     (id: 0);
  configured_mode:      ConfiguredMode = AUTO      (id: 1);
  runtime_status:       RuntimeStatus  = UNAVAILABLE (id: 2);
  degraded_reason:      string                     (id: 3);
  perf_event_name:      string                     (id: 4);
  perf_raw_count:       ulong = 0                  (id: 5);
  perf_time_enabled_ns: ulong = 0                  (id: 6);
  perf_time_running_ns: ulong = 0                  (id: 7);
}

table CollectorError {
  family:         string    (id: 0);
  detail:         string    (id: 1);
  observed_at_ns: ulong = 0 (id: 2);
}

table HostCpu {
  usage_percent_total:      float = 0 (id: 0);
  usage_percent_per_core:   [float]   (id: 1);
  context_switches_per_sec: ulong = 0 (id: 2);
  procs_running:            uint  = 0 (id: 3);
  procs_blocked:            uint  = 0 (id: 4);
}

table HostMemory {
  total_bytes:        ulong = 0 (id: 0);
  available_bytes:    ulong = 0 (id: 1);
  free_bytes:         ulong = 0 (id: 2);
  buffers_bytes:      ulong = 0 (id: 3);
  cached_bytes:       ulong = 0 (id: 4);
  swap_total_bytes:   ulong = 0 (id: 5);
  swap_free_bytes:    ulong = 0 (id: 6);
  pgfault_per_sec:    ulong = 0 (id: 7);
  pgmajfault_per_sec: ulong = 0 (id: 8);
  oom_kill_count:     ulong = 0 (id: 9);
  huge_pages_total:   ulong = 0 (id: 10);
  huge_pages_free:    ulong = 0 (id: 11);
}

table DiskStat {
  device:                string    (id: 0);
  read_bytes_per_sec:    ulong = 0 (id: 1);
  write_bytes_per_sec:   ulong = 0 (id: 2);
  read_ios_per_sec:      ulong = 0 (id: 3);
  write_ios_per_sec:     ulong = 0 (id: 4);
  discard_bytes_per_sec: ulong = 0 (id: 5);
  discard_ios_per_sec:   ulong = 0 (id: 6);
  inflight_reads:        ulong = 0 (id: 7);
  inflight_writes:       ulong = 0 (id: 8);
  await_ms:              float = 0 (id: 9);
  util_percent:          float = 0 (id: 10);
  avg_queue_length:      float = 0 (id: 11);
  logical_block_size:    uint  = 512 (id: 12);
}

table NetIfStat {
  ifname:             string     (id: 0);
  rx_bytes_per_sec:   ulong = 0  (id: 1);
  rx_packets_per_sec: ulong = 0  (id: 2);
  tx_bytes_per_sec:   ulong = 0  (id: 3);
  tx_packets_per_sec: ulong = 0  (id: 4);
  rx_errors:          ulong = 0  (id: 5);
  rx_drop:            ulong = 0  (id: 6);
  tx_errors:          ulong = 0  (id: 7);
  tx_drop:            ulong = 0  (id: 8);
  oper_state:         string     (id: 9);
  mtu:                uint  = 0  (id: 10);
  speed_mbps:         int   = -1 (id: 11);
}

table PsiLine {
  resource:       string    (id: 0);
  line:           string    (id: 1);
  avg10:          float = 0 (id: 2);
  avg60:          float = 0 (id: 3);
  avg300:         float = 0 (id: 4);
  total_us:       ulong = 0 (id: 5);
  total_delta_us: ulong = 0 (id: 6);
  stall_ratio:    float = 0 (id: 7);
}

table ProcessStat {
  pid:                                int        (id: 0);
  comm:                               string     (id: 1);
  cmdline:                            string     (id: 2);
  exe:                                string     (id: 3);
  cgroup_path:                        string     (id: 4);
  state:                              string     (id: 5);
  starttime_ticks:                    ulong = 0  (id: 6);
  thread_count:                       uint  = 0  (id: 7);
  cpu_user_us_delta:                  ulong = 0  (id: 8);
  cpu_kernel_us_delta:                ulong = 0  (id: 9);
  cpu_children_us_delta:              ulong = 0  (id: 10);
  cpu_percent_host:                   float = 0  (id: 11);
  cpu_percent_per_core:               float = 0  (id: 12);
  vss_bytes:                          ulong = 0  (id: 13);
  rss_bytes:                          ulong = 0  (id: 14);
  rss_hwm_bytes:                      ulong = 0  (id: 15);
  minor_faults_per_sec:               ulong = 0  (id: 16);
  major_faults_per_sec:               ulong = 0  (id: 17);
  voluntary_ctxt_switches_per_sec:    ulong = 0  (id: 18);
  nonvoluntary_ctxt_switches_per_sec: ulong = 0  (id: 19);
  read_bytes_per_sec:                 ulong = 0  (id: 20);
  write_bytes_per_sec:                ulong = 0  (id: 21);
  cancelled_write_bytes:              ulong = 0  (id: 22);
  fd_count:                           long  = -1 (id: 23);
  oom_score:                          int   = 0  (id: 24);
  oom_score_adj:                      int   = 0  (id: 25);
  exit_marker:                        bool  = false (id: 26);
}

table SysmonSnapshot {
  aorta_header:     AortaHeader        (id: 0);
  schema_version:   uint  = 1          (id: 1);
  host_name:        string             (id: 2);
  host_id:          string             (id: 3);
  kernel:           string             (id: 4);
  preempt_model:    string             (id: 5);
  cgroup_version:   ubyte = 0          (id: 6);
  online_cpus:      uint  = 0          (id: 7);
  monotonic_ns:     ulong = 0          (id: 8);
  interval_ms:      uint  = 0          (id: 9);
  sequence:         ulong = 0          (id: 10);
  capabilities:     [MetricCapability] (id: 11);
  collector_errors: [CollectorError]   (id: 12);
  cpu:              HostCpu            (id: 13);
  memory:           HostMemory         (id: 14);
  disks:            [DiskStat]         (id: 15);
  net_ifs:          [NetIfStat]        (id: 16);
  psi:              [PsiLine]          (id: 17);
  processes:        [ProcessStat]      (id: 18);
}

root_type SysmonSnapshot;
"#;

    fn build_fixture_sysmon_bfbs() -> Option<Vec<u8>> {
        build_bfbs_from(FIXTURE_FBS_SYSMON, "display_test_sysmon")
    }

    #[test]
    fn fb_to_json_display_sysmon_scale_payload() {
        // End-to-end reproduction of the original trigger shape against
        // the DisplayPrinter. Builds a SysmonSnapshot-structured payload
        // with values mirroring those that produced garbled output / SIGSEGV
        // in the original report (preempt_rt kernel, idle eth1, ACTIVE/
        // UNAVAILABLE/DEGRADED capability rows including one with the
        // declared default UNAVAILABLE omitted from the binary, an empty
        // vector, and a dense ProcessStat row).
        //
        // If this assertion ever fails on `DisplayPrinter`, the garbling bug is not
        // fully fixed and the renderer needs another look. Do not "fix" by
        // weakening the asserts.
        let Some(bfbs) = build_fixture_sysmon_bfbs() else {
            eprintln!("flatc not available; skipping");
            return;
        };

        let json = r#"{
            "aorta_header": {
                "publish_stamp_ns": 1700000000000000000,
                "publisher_node": "aorta-sysmon@robot-01",
                "sequence": 42
            },
            "schema_version": 1,
            "host_name": "robot-01",
            "host_id": "h-abc-123",
            "kernel": "Linux version 6.1.112-rt43",
            "preempt_model": "preempt_rt",
            "cgroup_version": 2,
            "online_cpus": 8,
            "monotonic_ns": 12345678901234,
            "interval_ms": 1000,
            "sequence": 42,
            "capabilities": [
                {"family": "cpu", "configured_mode": "ON", "runtime_status": "ACTIVE"},
                {"family": "psi_cpu", "configured_mode": "ON",
                 "runtime_status": "UNAVAILABLE",
                 "degraded_reason": "/proc/pressure/* missing"},
                {"family": "psi_memory", "configured_mode": "ON",
                 "degraded_reason": "/proc/pressure/* missing"},
                {"family": "zenoh_topology", "configured_mode": "OFF",
                 "runtime_status": "DEGRADED",
                 "degraded_reason": "configured off"}
            ],
            "collector_errors": [],
            "cpu": {
                "usage_percent_total": 3.5,
                "usage_percent_per_core": [2.1, 4.0, 3.7, 5.0],
                "context_switches_per_sec": 12345,
                "procs_running": 2,
                "procs_blocked": 0
            },
            "memory": {
                "total_bytes": 16777216,
                "available_bytes": 8388608,
                "free_bytes": 4194304
            },
            "disks": [
                {"device": "sda", "read_bytes_per_sec": 0, "write_bytes_per_sec": 0,
                 "logical_block_size": 4096}
            ],
            "net_ifs": [
                {"ifname": "eth0", "rx_bytes_per_sec": 1024, "oper_state": "up",
                 "mtu": 1500, "speed_mbps": 1000},
                {"ifname": "eth1", "rx_packets_per_sec": 0, "oper_state": "down",
                 "mtu": 1500, "speed_mbps": -1}
            ],
            "psi": [],
            "processes": [
                {"pid": 2161840, "comm": "aorta-sysmon", "cmdline": "/tmp/aorta-sysmon daemon",
                 "exe": "/tmp/aorta-sysmon", "cgroup_path": "/system.slice/aorta-sysmon",
                 "state": "R", "thread_count": 4,
                 "cpu_user_us_delta": 10000, "cpu_kernel_us_delta": 2000,
                 "cpu_percent_host": 1.2, "rss_bytes": 8388608,
                 "fd_count": 32, "oom_score": 0, "exit_marker": false}
            ]
        }"#;

        let fb = json_to_fb(
            &bfbs,
            json,
            Some("aorta.display_test.sysmon.SysmonSnapshot"),
        )
        .expect("json_to_fb succeeds on sysmon-shaped payload");
        let display =
            fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.sysmon.SysmonSnapshot"))
                .expect("fb_to_json_display must not fail on sysmon-shaped payload");

        // Top-level scalars decode with their actual values, not garbage.
        assert!(
            display.contains("host_name: \"robot-01\""),
            "host_name should decode verbatim: {display}"
        );
        assert!(
            display.contains("kernel: \"Linux version 6.1.112-rt43\""),
            "kernel string should decode verbatim: {display}"
        );
        assert!(
            display.contains("preempt_model: \"preempt_rt\""),
            "preempt_model should decode verbatim: {display}"
        );
        assert!(
            display.contains("online_cpus: 8"),
            "online_cpus must round-trip exactly: {display}"
        );
        assert!(
            display.contains("interval_ms: 1000"),
            "interval_ms must round-trip exactly: {display}"
        );

        // Non-first-variant enum default: the psi_memory row leaves
        // runtime_status at its declared default (UNAVAILABLE = 1), which the
        // wire elides. It is surfaced as UNAVAILABLE, and it
        // must never appear as ACTIVE (the zero/first variant) — that was the
        // original failure mode. We split the output at psi_memory's
        // family marker and inspect just that row's content.
        let psi_mem_idx = display
            .find("family: \"psi_memory\"")
            .expect("psi_memory row should be present");
        let psi_mem_end = display[psi_mem_idx..]
            .find('}')
            .map(|o| psi_mem_idx + o)
            .unwrap_or(display.len());
        let psi_mem_block = &display[psi_mem_idx..psi_mem_end];
        assert!(
            !psi_mem_block.contains("runtime_status: ACTIVE"),
            "absent runtime_status must NOT render as ACTIVE (this was the #77 bug): {psi_mem_block}"
        );

        // Stored non-default enum decodes to the declared variant.
        assert!(
            display.contains("runtime_status: DEGRADED"),
            "DEGRADED row must decode unquoted: {display}"
        );
        assert!(
            display.contains("runtime_status: ACTIVE"),
            "ACTIVE row (cpu) must decode unquoted: {display}"
        );
        assert!(
            display.contains("configured_mode: OFF"),
            "configured_mode enum must decode unquoted: {display}"
        );

        // Numeric scalars must not be garbled. The original bug showed obviously wrong
        // wire values for fields whose actual content is 0 (e.g. idle disk
        // `write_bytes_per_sec: 7881453968031772`, down interface
        // `rx_packets_per_sec: 8387109881047678976`). FlatBuffers elides
        // default-valued scalars from the wire; they are
        // surfaced as their default, so each 0-default field must render as
        // exactly `0` (never large numeric garbage). The `if let` below stays
        // tolerant of either behavior, but the value — when present — must be
        // exactly 0.
        let sda_idx = display
            .find("device: \"sda\"")
            .expect("sda disk row must be present");
        let sda_end = display[sda_idx..]
            .find('}')
            .map(|o| sda_idx + o)
            .unwrap_or(display.len());
        let sda_block = &display[sda_idx..sda_end];
        // logical_block_size is the only non-default scalar we set on sda.
        assert!(
            sda_block.contains("logical_block_size: 4096"),
            "sda.logical_block_size must round-trip: {sda_block}"
        );
        // Default-zero throughput fields must not be misrendered as garbage.
        for garbage_field in [
            "read_bytes_per_sec:",
            "write_bytes_per_sec:",
            "read_ios_per_sec:",
            "write_ios_per_sec:",
        ] {
            if let Some(off) = sda_block.find(garbage_field) {
                let after = &sda_block[off + garbage_field.len()..];
                let rendered = after.trim_start();
                assert!(
                    rendered.starts_with("0,")
                        || rendered.starts_with("0\n")
                        || rendered.starts_with("0 "),
                    "{garbage_field} must be absent or exactly 0, got: {sda_block}"
                );
            }
        }
        let eth1_idx = display
            .find("ifname: \"eth1\"")
            .expect("eth1 interface row must be present");
        let eth1_end = display[eth1_idx..]
            .find('}')
            .map(|o| eth1_idx + o)
            .unwrap_or(display.len());
        let eth1_block = &display[eth1_idx..eth1_end];
        // oper_state + mtu are non-default values on eth1; both should
        // round-trip. (speed_mbps = -1 matches the schema default; it
        // is surfaced at that default rather than omitted.)
        assert!(
            eth1_block.contains("oper_state: \"down\""),
            "eth1.oper_state must round-trip: {eth1_block}"
        );
        assert!(
            eth1_block.contains("mtu: 1500"),
            "eth1.mtu must round-trip: {eth1_block}"
        );
        if let Some(off) = eth1_block.find("rx_packets_per_sec:") {
            let after = &eth1_block[off + "rx_packets_per_sec:".len()..];
            let rendered = after.trim_start();
            assert!(
                rendered.starts_with("0,")
                    || rendered.starts_with("0\n")
                    || rendered.starts_with("0 "),
                "down interface rx_packets_per_sec must be absent or exactly 0, got: {eth1_block}"
            );
        }

        // Empty vectors round-trip.
        assert!(
            display.contains("collector_errors: []"),
            "empty collector_errors vector must render []: {display}"
        );
        assert!(
            display.contains("psi: []"),
            "empty psi vector must render []: {display}"
        );

        // Nested AortaHeader sub-table renders intact.
        assert!(
            display.contains("publisher_node: \"aorta-sysmon@robot-01\""),
            "AortaHeader.publisher_node must decode: {display}"
        );

        // Dense ProcessStat row: even with explicit (id: N) up through 26 and
        // many omitted-because-default fields, the present fields must decode.
        assert!(
            display.contains("comm: \"aorta-sysmon\""),
            "ProcessStat.comm must decode: {display}"
        );
        assert!(
            display.contains("rss_bytes: 8388608"),
            "ProcessStat.rss_bytes must decode: {display}"
        );

        // Brace balance — corrupt rendering historically dropped closers.
        assert_eq!(
            display.matches('{').count(),
            display.matches('}').count(),
            "unbalanced braces in sysmon-scale output: {display}"
        );
        // Bracket balance for vectors.
        assert_eq!(
            display.matches('[').count(),
            display.matches(']').count(),
            "unbalanced brackets in sysmon-scale output: {display}"
        );
    }

    // ── (vector64) regression ────────────────────────────────────────────
    //
    // Schema with a `[ubyte] (vector64)` field. The pointer to the vector
    // is stored at the table's vtable slot as an `Offset64` (8 bytes); the
    // vector header itself begins with a `uoffset64_t` length (8 bytes).
    // The renderer was broken on both reads:
    //   1. `PrintVec` defaulted `SizeT = uoffset_t` (4 bytes), so the
    //      vector length was read as 4 bytes — fixed by `PrintVecBySize`.
    //   2. `GenFieldOffset` used `table->GetPointer<const void*>` for all
    //      non-struct pointer fields, which reads a 4-byte offset; for a
    //      `(vector64)` field the slot is 8 bytes wide — fixed by
    //      dispatching on `fd.offset64` to `GetPointer64`.
    //
    // What the small-fixture tests below DO cover: the renderer walks an
    // `(vector64)` field via the 64-bit dispatch and produces correct
    // output for typical payloads (length + element traversal + adjacent
    // field round-trip).
    //
    // What they do NOT cover: producing an offset value whose high 32
    // bits are non-zero. That requires the destination to sit > 4 GiB
    // from the field slot, which in turn requires a > 4 GiB buffer; for
    // any buffer ≤ 4 GiB the 4-byte and 8-byte reads of the slot return
    // the same value (high 32 bits are zero) and the bug is silently
    // benign. The fix is verified structurally via
    // `fb_to_json_display_vector64_schema_marks_offset64`, which asserts
    // the bfbs reflection has `Field::offset64() == true` for the
    // `payload` field, plus by code inspection against `fd.offset64` in
    // `GenFieldOffset`.
    //
    // Schema notes:
    //   - `attributes.fbs` is included to whitelist `(vector64)` so the
    //     parser accepts it. (The runtime currently has zero `(vector64)`
    //     fields under `message/schemas/**`, hence the latent risk this test
    //     closes.)
    //   - flatc's text parser supports `(vector64)` on `[ubyte]` and a few
    //     other element types; we use ubyte because that path is the most
    //     widely supported across upstream releases.
    const FIXTURE_FBS_VECTOR64: &str = r#"
include "attributes.fbs";

namespace aorta.display_test;

table BlobV64 {
  payload: [ubyte] (vector64);
  tag: string;
}

root_type BlobV64;
"#;

    fn build_fixture_vector64_bfbs() -> Option<Vec<u8>> {
        let flatc = resolve_flatc()?;
        let dir = {
            static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::env::temp_dir().join(format!(
                "aorta-display-test-v64-{}-{}",
                std::process::id(),
                seq
            ))
        };
        std::fs::create_dir_all(&dir).ok()?;
        // `(vector64)` is a built-in attribute but flatc requires it to be
        // declared via `attribute "vector64";` or via the bundled
        // attributes.fbs include before use in a schema text. Write a
        // minimal attributes.fbs alongside the fixture.
        std::fs::write(dir.join("attributes.fbs"), "attribute \"vector64\";\n").ok()?;
        let fbs_path = dir.join("display_test_vector64.fbs");
        std::fs::write(&fbs_path, FIXTURE_FBS_VECTOR64).ok()?;
        let status = std::process::Command::new(&flatc)
            .arg("--binary")
            .arg("--schema")
            .arg("--bfbs-builtins")
            .arg("-o")
            .arg(&dir)
            .arg(&fbs_path)
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }
        std::fs::read(dir.join("display_test_vector64.bfbs")).ok()
    }

    #[test]
    fn fb_to_json_display_vector64_field() {
        // Regression coverage: a `(vector64)` field must render its
        // length and elements correctly. Before the SizeT specialization,
        // the 8-byte length was read as 4 bytes, producing garbage size and
        // (often) OOB reads on the element data.
        let Some(bfbs) = build_fixture_vector64_bfbs() else {
            eprintln!("flatc not available or (vector64) not supported; skipping");
            return;
        };

        // Non-empty payload — verifies length read + element traversal both
        // work under the 64-bit SizeT path.
        let json = r#"{"payload": [1, 2, 3, 250, 255], "tag": "ok"}"#;
        let fb = json_to_fb(&bfbs, json, Some("aorta.display_test.BlobV64"))
            .expect("json_to_fb succeeds for (vector64) ubyte payload");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.BlobV64"))
            .expect("fb_to_json_display must not fail on (vector64) field");

        // tag must round-trip — corruption in payload length used to spill
        // into adjacent fields, mangling the tag.
        assert!(
            display.contains("tag: \"ok\""),
            "tag must round-trip after (vector64) payload: {display}"
        );
        // All five payload bytes must appear exactly as standalone numeric
        // tokens — a plain `display.contains("2")` would match inside "250".
        let tokens: Vec<&str> = display
            .split(|c: char| !c.is_ascii_digit())
            .filter(|s| !s.is_empty())
            .collect();
        for byte in ["1", "2", "3", "250", "255"] {
            assert!(
                tokens.iter().any(|t| *t == byte),
                "payload byte {byte} must appear as standalone token in output: {display}"
            );
        }
        // Bracket balance — corruption used to drop the closing `]`.
        assert_eq!(
            display.matches('[').count(),
            display.matches(']').count(),
            "unbalanced brackets after (vector64) decode: {display}"
        );
    }

    #[test]
    fn fb_to_json_display_vector64_empty_field() {
        // Empty `(vector64)` field — length-read alone must work even when
        // there are no elements. This is the simplest case that still
        // distinguishes 4-byte from 8-byte length decoding (a zero u64 read
        // as u32 is still zero, but the offset arithmetic past the length
        // differs by 4 bytes and used to misalign `tag`).
        let Some(bfbs) = build_fixture_vector64_bfbs() else {
            eprintln!("flatc not available or (vector64) not supported; skipping");
            return;
        };
        let json = r#"{"payload": [], "tag": "empty"}"#;
        let fb = json_to_fb(&bfbs, json, Some("aorta.display_test.BlobV64"))
            .expect("json_to_fb succeeds for empty (vector64) field");
        let display = fb_to_json_display(&bfbs, &fb, Some("aorta.display_test.BlobV64"))
            .expect("fb_to_json_display must not fail on empty (vector64) field");

        assert!(
            display.contains("payload: []"),
            "empty (vector64) must render as []: {display}"
        );
        assert!(
            display.contains("tag: \"empty\""),
            "tag must round-trip alongside empty (vector64): {display}"
        );
    }

    #[test]
    fn fb_to_json_display_vector64_schema_marks_offset64() {
        // Structural regression for the `GenFieldOffset` dispatch on
        // `fd.offset64`. This test is the closest practical proxy for a
        // "force the Offset64 parse path" assertion: it asserts the
        // fixture's `payload` field is actually marked `offset64() == true`
        // in the bfbs reflection — i.e. the runtime FieldDef the C++
        // renderer reads at GenFieldOffset will have `fd.offset64== true`
        // for this field, so the GetPointer64 branch IS exercised on every
        // run of the other `(vector64)` tests.
        //
        // We cannot construct an offset whose high 32 bits diverge from
        // the low 32 bits without a > 4 GiB buffer (see the block comment
        // above FIXTURE_FBS_VECTOR64). Without that divergence, the buggy
        // 4-byte read and the fixed 8-byte read return the same value on
        // any feasibly-sized fixture, so the existing render tests cannot
        // distinguish the two. This schema-side assertion locks in the
        // precondition that makes the GetPointer64 dispatch actually fire.
        use flatbuffers_reflection::reflection::root_as_schema;
        let Some(bfbs) = build_fixture_vector64_bfbs() else {
            eprintln!("flatc not available or (vector64) not supported; skipping");
            return;
        };
        let schema = root_as_schema(&bfbs).expect("bfbs must parse as a Schema");
        let object = schema
            .objects()
            .iter()
            .find(|o| o.name() == "aorta.display_test.BlobV64")
            .expect("schema must contain BlobV64");
        let payload = object
            .fields()
            .iter()
            .find(|f| f.name() == "payload")
            .expect("BlobV64 must declare a payload field");
        assert!(
            payload.offset64(),
            "the bfbs reflection must mark BlobV64.payload as offset64; \
             without this flag, GenFieldOffset will fall back to the 32-bit \
             GetPointer path and the (vector64) regression coverage is void"
        );
    }
}
