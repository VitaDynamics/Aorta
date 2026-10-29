//! Process-wide output format control for the `aorta` CLI.
//!
//! The `--format text|json` global flag is stored once in an `AtomicU8` (mirroring
//! the `CLIENT_MODE_OVERRIDE` pattern in `cmd::mod`) so command implementations can
//! branch on it without threading an extra parameter through every signature.
//!
//! - Text mode (default): human banners go to stderr, data rows to stdout.
//! - JSON mode: discovery/list commands emit a single pretty-printed JSON document
//!   to stdout and skip the human banner/table entirely.

use std::sync::atomic::{AtomicU8, Ordering};

/// Output rendering mode selected by the global `--format` flag.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text (default): banners on stderr, data on stdout.
    #[default]
    Text,
    /// Machine-readable JSON on stdout (discovery/list commands only for now).
    Json,
}

const FORMAT_TEXT: u8 = 0;
const FORMAT_JSON: u8 = 1;

static OUTPUT_FORMAT: AtomicU8 = AtomicU8::new(FORMAT_TEXT);

/// Store the process-wide output format. Called once from `main()` after parsing.
pub fn set_output_format(format: OutputFormat) {
    let encoded = match format {
        OutputFormat::Text => FORMAT_TEXT,
        OutputFormat::Json => FORMAT_JSON,
    };
    OUTPUT_FORMAT.store(encoded, Ordering::Relaxed);
}

/// Read the process-wide output format. Defaults to [`OutputFormat::Text`].
pub fn output_format() -> OutputFormat {
    match OUTPUT_FORMAT.load(Ordering::Relaxed) {
        FORMAT_JSON => OutputFormat::Json,
        _ => OutputFormat::Text,
    }
}

/// Pretty-print `value` as JSON to stdout (one document, trailing newline).
pub fn emit_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{emit_json, output_format, set_output_format, OutputFormat};

    #[test]
    fn output_format_defaults_to_text() {
        assert_eq!(OutputFormat::default(), OutputFormat::Text);
    }

    #[test]
    fn set_and_read_output_format_round_trips() {
        // Note: OUTPUT_FORMAT is process-global; restore Text afterward so other
        // tests in this binary observe the default.
        set_output_format(OutputFormat::Json);
        assert_eq!(output_format(), OutputFormat::Json);
        set_output_format(OutputFormat::Text);
        assert_eq!(output_format(), OutputFormat::Text);
    }

    #[test]
    fn emit_json_renders_pretty_shape() {
        // emit_json prints to stdout; here we exercise the same serialization path
        // it uses and assert the pretty (multi-line, 2-space indented) shape.
        let value = serde_json::json!([{"name": "a", "n": 1}]);
        assert!(emit_json(&value).is_ok());
        let rendered = serde_json::to_string_pretty(&value).expect("pretty");
        assert_eq!(
            rendered,
            "[\n  {\n    \"n\": 1,\n    \"name\": \"a\"\n  }\n]"
        );
    }
}
