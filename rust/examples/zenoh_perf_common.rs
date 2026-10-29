#[derive(Clone, Copy, Debug)]
pub enum PayloadFormat {
    Raw,
    Flatbuffer,
}

pub fn normalize_zenoh_topic(topic: &str) -> Result<String, String> {
    let normalized = topic.trim().trim_matches('/');
    if normalized.is_empty() {
        return Err("topic must contain at least one non-slash segment".to_string());
    }
    Ok(normalized.to_string())
}

pub fn parse_format(value: &str) -> Option<PayloadFormat> {
    match value.to_ascii_lowercase().as_str() {
        "raw" => Some(PayloadFormat::Raw),
        "flatbuffer" | "fb" => Some(PayloadFormat::Flatbuffer),
        _ => None,
    }
}

pub fn config_from_env_or_default(
) -> Result<zenoh::Config, Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(raw) = std::env::var("ZENOH_SESSION_CONFIG_URI") {
        let raw = raw.trim();
        if !raw.is_empty() {
            let mut path = raw;
            if let Some(stripped) = raw.strip_prefix("file://") {
                path = stripped;
                if let Some(local) = path.strip_prefix("localhost/") {
                    path = local;
                }
            }
            return Ok(zenoh::Config::from_file(path)?);
        }
    }
    if std::env::var_os(zenoh::Config::DEFAULT_CONFIG_PATH_ENV).is_some() {
        return Ok(zenoh::Config::from_env()?);
    }
    Ok(zenoh::Config::default())
}
