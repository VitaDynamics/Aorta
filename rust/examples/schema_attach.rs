use aorta_sdk::SchemaAttachPolicy;

pub fn parse_schema_attach_policy(value: &str) -> Option<SchemaAttachPolicy> {
    match value {
        "never" => Some(SchemaAttachPolicy::Never),
        "always" => Some(SchemaAttachPolicy::Always),
        "inherit" => Some(SchemaAttachPolicy::Inherit),
        _ => None,
    }
}
