use anyhow::anyhow;
use factordb::{data::ValueType, AnyError};
use inflector::Inflector;

/// Generate Typescript type definitions for a database schema.
pub fn schema_to_typescript(
    schema: &factordb::schema::DbSchema,
    factor_import_path: Option<&str>,
) -> Result<String, AnyError> {
    let mut out = String::new();

    out.push_str("// This file was auto-generated with factordb tooling.\n");
    out.push_str("// You probably don't want to edit manually.\n\n");

    if let Some(source) = factor_import_path {
        out.push_str(&format!(
            "import {{EntityId, Url, Timestamp, BaseEntityData}} from \"{}\";\n\n",
            source
        ));
    } else {
        out.push_str(
            r#"
type EntityId = string;
type Url = string;
type Timestamp = number;

interface BaseEntityData {
  "factor/id": EntityId,
  "factor/type"?: string | null,
}
        "#,
        );
    };

    out.push_str("// Attributes\n\n");
    for attr in &schema.attributes {
        let name = attr.ident.replace('/', "_").to_screaming_snake_case();
        out.push_str(&format!(
            "export const ATTR_{} = \"{}\";\n",
            name, attr.ident
        ));
    }

    out.push_str("// Entities\n\n");
    for entity in &schema.entities {
        let name = entity.ident.replace('/', "_").to_class_case();

        let extends_names = entity
            .extends
            .iter()
            .map(|parent_ident| -> Result<_, AnyError> {
                let parent = schema
                    .resolve_entity(parent_ident)
                    .ok_or_else(|| anyhow!("Could not find entity '{:?}'", parent_ident))?;

                Ok(parent.ident.replace('/', "_").to_class_case())
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");

        let extends = if !extends_names.is_empty() {
            format!("extends {} ", extends_names)
        } else {
            format!("extends BaseEntityData ")
        };

        out.push_str(&format!("export interface {} {}{{\n", name, extends));

        out.push_str(&format!("  \"factor/type\": \"{}\",\n", entity.ident));

        for field in &entity.attributes {
            // TODO: extract object types into separate definitions.

            let attr = schema.resolve_attr(&field.attribute).ok_or_else(|| {
                anyhow!("Could not find attribute {:?} in schema", field.attribute)
            })?;

            let ty = field_ts_type(field, attr);

            let name = if attr.ident.contains('/') {
                format!("\"{}\"", attr.ident)
            } else {
                attr.ident.clone()
            };

            let optional_marker = if field.cardinality.is_optional() {
                "?"
            } else {
                ""
            };

            out.push_str(&format!("  {}{}: {},\n", name, optional_marker, ty));
        }

        out.push_str("}\n\n");
    }

    Ok(out)
}

fn field_ts_type(
    field: &factordb::schema::EntityAttribute,
    attr: &factordb::schema::AttributeSchema,
) -> String {
    let inner = value_to_ts_type(&attr.value_type);
    match field.cardinality {
        factordb::schema::Cardinality::Optional => format!("{} | null", inner),
        factordb::schema::Cardinality::Required => inner,
        factordb::schema::Cardinality::Many => format!("{}[]", inner),
    }
}

fn value_to_ts_type(ty: &ValueType) -> String {
    match ty {
        ValueType::Any => "any".to_string(),
        ValueType::Unit => "void".to_string(),
        ValueType::Bool => "boolean".to_string(),
        ValueType::Int | ValueType::UInt | ValueType::Float => "number".to_string(),
        ValueType::String => "string".to_string(),
        // TODO: how to represent byte arrays?
        ValueType::Bytes => "number[]".to_string(),
        ValueType::List(inner) => format!("{}[]", value_to_ts_type(inner)),
        ValueType::Map => "Record<string, any>".to_string(),
        ValueType::Union(variants) => variants
            .iter()
            .map(value_to_ts_type)
            .collect::<Vec<_>>()
            .join(" | "),
        ValueType::Object(obj) => {
            let fields = obj
                .fields
                .iter()
                .map(|field| format!("  {}: {},", field.name, value_to_ts_type(&field.value_type)))
                .collect::<Vec<_>>()
                .join("\n");
            format!("{{\n{}\n}}", fields)
        }
        ValueType::DateTime => "Timestamp".to_string(),
        ValueType::Url => "Url".to_string(),
        ValueType::Ref => "EntityId".to_string(),
    }
}
