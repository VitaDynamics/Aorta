use std::sync::Arc;

use aorta_core::schema::{bfbs_root_type_name, infer_service_root_types_from_bfbs, SchemaHash};
use aorta_core::schema_registry::SchemaKind;
use flatbuffers_reflection::reflection::{self, BaseType, Enum, Field, Object, Type};
use serde_json::Value;

use crate::error::{IntrospectError, IntrospectResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaRootSelector {
    DefaultRoot,
    NamedRoot(String),
    ServiceRequest,
    ServiceResponse,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaModel {
    pub root_type: String,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaField {
    pub path: String,
    pub name: String,
    pub parent_path: Option<String>,
    pub depth: usize,
    pub type_name: String,
    pub type_kind: SchemaTypeKind,
    pub optional: bool,
    pub repeated: bool,
    pub default_json: Option<Value>,
    pub enum_values: Vec<SchemaEnumValue>,
    pub doc: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaEnumValue {
    pub name: String,
    pub number: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaTypeKind {
    Bool,
    Integer,
    Float,
    String,
    Bytes,
    Table,
    Struct,
    Enum,
    Union,
    Vector,
    Array,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedTopicSchemaKind {
    Topic,
    Param,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTopicSchema {
    pub key_suffix: String,
    pub kind: ResolvedTopicSchemaKind,
    pub schema_hash: String,
    pub bfbs: Arc<[u8]>,
    pub default_root_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedServiceSchema {
    pub key_suffix: String,
    pub service_hash: String,
    pub bfbs: Arc<[u8]>,
    pub request_root_type: Option<String>,
    pub response_root_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedSchema {
    Topic(ResolvedTopicSchema),
    Service(ResolvedServiceSchema),
}

impl ResolvedTopicSchema {
    pub fn new(key_suffix: impl Into<String>, bfbs: Vec<u8>) -> Self {
        Self::with_kind(key_suffix, ResolvedTopicSchemaKind::Topic, bfbs)
    }

    pub fn with_kind(
        key_suffix: impl Into<String>,
        kind: ResolvedTopicSchemaKind,
        bfbs: Vec<u8>,
    ) -> Self {
        Self::with_kind_and_root_type(key_suffix, kind, bfbs, None)
    }

    pub fn with_kind_and_root_type(
        key_suffix: impl Into<String>,
        kind: ResolvedTopicSchemaKind,
        bfbs: Vec<u8>,
        wire_root_type: Option<String>,
    ) -> Self {
        let hash = SchemaHash::from_bfbs_bytes(&bfbs).to_hex();
        let default_root_type = wire_root_type.or_else(|| bfbs_root_type_name(&bfbs));
        Self {
            key_suffix: key_suffix.into(),
            kind,
            schema_hash: hash,
            bfbs: Arc::from(bfbs),
            default_root_type,
        }
    }
}

impl From<SchemaKind> for ResolvedTopicSchemaKind {
    fn from(value: SchemaKind) -> Self {
        match value {
            SchemaKind::Topic => Self::Topic,
            SchemaKind::Param => Self::Param,
            SchemaKind::Service => Self::Topic,
        }
    }
}

impl ResolvedServiceSchema {
    pub fn new(
        key_suffix: impl Into<String>,
        bfbs: Vec<u8>,
        request_root_type: Option<String>,
        response_root_type: Option<String>,
    ) -> Self {
        let service_hash = SchemaHash::from_bfbs_bytes(&bfbs).to_hex();
        let (request_root_type, response_root_type) = infer_service_root_types_from_bfbs(
            &bfbs,
            request_root_type.as_deref(),
            response_root_type.as_deref(),
        );
        Self {
            key_suffix: key_suffix.into(),
            service_hash,
            bfbs: Arc::from(bfbs),
            request_root_type,
            response_root_type,
        }
    }
}

pub fn build_schema_model(
    schema: &ResolvedSchema,
    root_selector: SchemaRootSelector,
) -> IntrospectResult<SchemaModel> {
    let (bfbs, root_name) = match schema {
        ResolvedSchema::Topic(topic) => {
            let root_name = match root_selector {
                SchemaRootSelector::DefaultRoot => topic.default_root_type.clone(),
                SchemaRootSelector::NamedRoot(ref root_name) => Some(root_name.clone()),
                SchemaRootSelector::ServiceRequest | SchemaRootSelector::ServiceResponse => None,
            };
            (topic.bfbs.as_ref(), root_name)
        }
        ResolvedSchema::Service(service) => {
            let root_name = match root_selector {
                SchemaRootSelector::DefaultRoot => service.request_root_type.clone(),
                SchemaRootSelector::NamedRoot(ref root_name) => Some(root_name.clone()),
                SchemaRootSelector::ServiceRequest => service.request_root_type.clone(),
                SchemaRootSelector::ServiceResponse => service.response_root_type.clone(),
            };
            (service.bfbs.as_ref(), root_name)
        }
    };

    let root_name = root_name.ok_or_else(|| {
        IntrospectError::decode("schema root type is unavailable for the requested selector")
    })?;

    let schema = reflection::root_as_schema(bfbs)
        .map_err(|err| IntrospectError::decode(format!("failed to parse BFBS schema: {}", err)))?;
    let root_object = find_object_by_name(&schema, &root_name).ok_or_else(|| {
        IntrospectError::decode(format!(
            "root object '{}' not found in BFBS schema",
            root_name
        ))
    })?;

    let mut fields = Vec::new();
    build_object_fields(&schema, root_object, None, 0, &mut fields)?;

    Ok(SchemaModel {
        root_type: root_name,
        fields,
    })
}

fn build_object_fields(
    schema: &reflection::Schema<'_>,
    object: Object<'_>,
    parent_path: Option<&str>,
    depth: usize,
    out: &mut Vec<SchemaField>,
) -> IntrospectResult<()> {
    for field in object.fields().iter() {
        let path = join_path(parent_path, field.name());
        let field_type = field.type_();
        let (type_name, type_kind, enum_values) = describe_type(schema, field_type)?;
        let default_json = scalar_default_json(field, field_type);
        let doc = join_doc_lines(field.documentation());

        out.push(SchemaField {
            path: path.clone(),
            name: field.name().to_string(),
            parent_path: parent_path.map(str::to_string),
            depth,
            type_name,
            type_kind,
            optional: field.optional(),
            repeated: matches!(
                field_type.base_type(),
                BaseType::Vector | BaseType::Vector64 | BaseType::Array
            ),
            default_json,
            enum_values,
            doc,
        });

        if matches!(field_type.base_type(), BaseType::Obj) {
            if let Some(child) = object_for_type(schema, field_type) {
                build_object_fields(schema, child, Some(&path), depth + 1, out)?;
            }
        }
    }
    Ok(())
}

fn describe_type(
    schema: &reflection::Schema<'_>,
    field_type: Type<'_>,
) -> IntrospectResult<(String, SchemaTypeKind, Vec<SchemaEnumValue>)> {
    match field_type.base_type() {
        BaseType::Bool => Ok(("bool".to_string(), SchemaTypeKind::Bool, Vec::new())),
        BaseType::Byte => Ok(("i8".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::UByte => Ok(("u8".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::Short => Ok(("i16".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::UShort => Ok(("u16".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::Int => Ok(("i32".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::UInt => Ok(("u32".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::Long => Ok(("i64".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::ULong => Ok(("u64".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::Float => Ok(("f32".to_string(), SchemaTypeKind::Float, Vec::new())),
        BaseType::Double => Ok(("f64".to_string(), SchemaTypeKind::Float, Vec::new())),
        BaseType::String => Ok(("string".to_string(), SchemaTypeKind::String, Vec::new())),
        BaseType::Vector | BaseType::Vector64 => {
            if field_type.element() == BaseType::UByte {
                return Ok(("bytes".to_string(), SchemaTypeKind::Bytes, Vec::new()));
            }
            let (element_name, _, _) = describe_element_type(schema, field_type)?;
            Ok((
                format!("[{}]", element_name),
                SchemaTypeKind::Vector,
                Vec::new(),
            ))
        }
        BaseType::Array => {
            let (element_name, _, _) = describe_element_type(schema, field_type)?;
            Ok((
                format!("[{}:{}]", element_name, field_type.fixed_length()),
                SchemaTypeKind::Array,
                Vec::new(),
            ))
        }
        BaseType::Obj => {
            let object = object_for_type(schema, field_type).ok_or_else(|| {
                IntrospectError::decode(format!(
                    "object index {} not found in BFBS",
                    field_type.index()
                ))
            })?;
            let kind = if object.is_struct() {
                SchemaTypeKind::Struct
            } else {
                SchemaTypeKind::Table
            };
            Ok((object.name().to_string(), kind, Vec::new()))
        }
        BaseType::Union => {
            let enum_def = enum_for_type(schema, field_type).ok_or_else(|| {
                IntrospectError::decode(format!(
                    "union index {} not found in BFBS",
                    field_type.index()
                ))
            })?;
            Ok((
                enum_def.name().to_string(),
                SchemaTypeKind::Union,
                collect_enum_values(enum_def),
            ))
        }
        other => {
            if let Some(enum_def) = enum_for_type(schema, field_type) {
                return Ok((
                    enum_def.name().to_string(),
                    SchemaTypeKind::Enum,
                    collect_enum_values(enum_def),
                ));
            }
            Err(IntrospectError::decode(format!(
                "unsupported FlatBuffers base type: {:?}",
                other
            )))
        }
    }
}

fn describe_element_type(
    schema: &reflection::Schema<'_>,
    field_type: Type<'_>,
) -> IntrospectResult<(String, SchemaTypeKind, Vec<SchemaEnumValue>)> {
    match field_type.element() {
        BaseType::Bool => Ok(("bool".to_string(), SchemaTypeKind::Bool, Vec::new())),
        BaseType::Byte => Ok(("i8".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::UByte => Ok(("u8".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::Short => Ok(("i16".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::UShort => Ok(("u16".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::Int => Ok(("i32".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::UInt => Ok(("u32".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::Long => Ok(("i64".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::ULong => Ok(("u64".to_string(), SchemaTypeKind::Integer, Vec::new())),
        BaseType::Float => Ok(("f32".to_string(), SchemaTypeKind::Float, Vec::new())),
        BaseType::Double => Ok(("f64".to_string(), SchemaTypeKind::Float, Vec::new())),
        BaseType::String => Ok(("string".to_string(), SchemaTypeKind::String, Vec::new())),
        BaseType::Obj => {
            let object = object_for_index(schema, field_type.index()).ok_or_else(|| {
                IntrospectError::decode(format!(
                    "object index {} not found in BFBS",
                    field_type.index()
                ))
            })?;
            let kind = if object.is_struct() {
                SchemaTypeKind::Struct
            } else {
                SchemaTypeKind::Table
            };
            Ok((object.name().to_string(), kind, Vec::new()))
        }
        BaseType::Union => {
            let enum_def = enum_for_index(schema, field_type.index()).ok_or_else(|| {
                IntrospectError::decode(format!(
                    "union index {} not found in BFBS",
                    field_type.index()
                ))
            })?;
            Ok((
                enum_def.name().to_string(),
                SchemaTypeKind::Union,
                collect_enum_values(enum_def),
            ))
        }
        other => {
            if let Some(enum_def) = enum_for_index(schema, field_type.index()) {
                return Ok((
                    enum_def.name().to_string(),
                    SchemaTypeKind::Enum,
                    collect_enum_values(enum_def),
                ));
            }
            Err(IntrospectError::decode(format!(
                "unsupported FlatBuffers element type: {:?}",
                other
            )))
        }
    }
}

fn object_for_type<'a>(
    schema: &'a reflection::Schema<'a>,
    field_type: Type<'a>,
) -> Option<Object<'a>> {
    object_for_index(schema, field_type.index())
}

fn enum_for_type<'a>(schema: &'a reflection::Schema<'a>, field_type: Type<'a>) -> Option<Enum<'a>> {
    enum_for_index(schema, field_type.index())
}

fn object_for_index<'a>(schema: &'a reflection::Schema<'a>, index: i32) -> Option<Object<'a>> {
    let index = usize::try_from(index).ok()?;
    let objects = schema.objects();
    if index >= objects.len() {
        None
    } else {
        Some(objects.get(index))
    }
}

fn enum_for_index<'a>(schema: &'a reflection::Schema<'a>, index: i32) -> Option<Enum<'a>> {
    let index = usize::try_from(index).ok()?;
    let enums = schema.enums();
    if index >= enums.len() {
        None
    } else {
        Some(enums.get(index))
    }
}

fn find_object_by_name<'a>(schema: &'a reflection::Schema<'a>, name: &str) -> Option<Object<'a>> {
    schema.objects().iter().find(|object| object.name() == name)
}

fn collect_enum_values(enum_def: Enum<'_>) -> Vec<SchemaEnumValue> {
    enum_def
        .values()
        .iter()
        .map(|value| SchemaEnumValue {
            name: value.name().to_string(),
            number: value.value(),
        })
        .collect()
}

fn scalar_default_json(field: Field<'_>, field_type: Type<'_>) -> Option<Value> {
    match field_type.base_type() {
        BaseType::Bool => Some(Value::Bool(field.default_integer() != 0)),
        BaseType::Byte | BaseType::Short | BaseType::Int | BaseType::Long => {
            Some(Value::from(field.default_integer()))
        }
        BaseType::UByte | BaseType::UShort | BaseType::UInt | BaseType::ULong => {
            Some(Value::from(field.default_integer()))
        }
        BaseType::Float | BaseType::Double => Some(Value::from(field.default_real())),
        _ => None,
    }
}

fn join_doc_lines(
    documentation: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<&str>>>,
) -> Option<String> {
    let docs = documentation?;
    let lines: Vec<_> = docs
        .iter()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
    }
}

fn join_path(parent_path: Option<&str>, name: &str) -> String {
    if let Some(parent_path) = parent_path {
        format!("{}.{}", parent_path, name)
    } else {
        name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        build_schema_model, ResolvedSchema, ResolvedServiceSchema, ResolvedTopicSchema,
        ResolvedTopicSchemaKind, SchemaRootSelector,
    };

    #[test]
    fn resolved_topic_schema_derives_hash_even_for_dummy_bfbs() {
        let schema = ResolvedTopicSchema::new("pub/demo/topic", vec![1, 2, 3, 4]);
        assert_eq!(schema.key_suffix, "pub/demo/topic");
        assert_eq!(schema.schema_hash.len(), 64);
    }

    #[test]
    fn resolved_service_schema_keeps_missing_roots_when_bfbs_invalid() {
        let schema = ResolvedServiceSchema::new("srv/demo/echo", vec![1, 2, 3], None, None);
        assert_eq!(schema.key_suffix, "srv/demo/echo");
        assert!(schema.request_root_type.is_none());
        assert!(schema.response_root_type.is_none());
    }

    #[test]
    fn build_schema_model_rejects_invalid_bfbs() {
        let schema = ResolvedSchema::Topic(ResolvedTopicSchema {
            key_suffix: "pub/demo/topic".to_string(),
            kind: ResolvedTopicSchemaKind::Topic,
            schema_hash: "0".repeat(64),
            bfbs: Arc::from([1u8, 2u8, 3u8].as_slice()),
            default_root_type: Some("demo.Topic".to_string()),
        });
        let err = build_schema_model(&schema, SchemaRootSelector::DefaultRoot)
            .expect_err("invalid bfbs should fail");
        assert!(err.to_string().contains("failed to parse BFBS schema"));
    }

    #[test]
    fn build_schema_model_rejects_missing_service_response_root() {
        let schema = ResolvedSchema::Service(ResolvedServiceSchema {
            key_suffix: "srv/demo/echo".to_string(),
            service_hash: "1".repeat(64),
            bfbs: Arc::from([1u8, 2u8, 3u8].as_slice()),
            request_root_type: Some("demo.EchoRequest".to_string()),
            response_root_type: None,
        });
        let err = build_schema_model(&schema, SchemaRootSelector::ServiceResponse)
            .expect_err("missing response root should fail");
        assert!(err.to_string().contains("root type is unavailable"));
    }
}
