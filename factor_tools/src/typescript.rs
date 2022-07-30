use std::collections::HashSet;

use anyhow::anyhow;
use factordb::{data::ValueType, AnyError};
use inflector::Inflector;

/// Generate Typescript type definitions for a database schema.
pub fn schema_to_typescript(
    schema: &factordb::schema::DbSchema,
    factor_import_path: Option<&str>,
) -> Result<String, AnyError> {
    let mut module = Module { items: Vec::new() };

    module.add(Item::Comment(
        "This file was auto-generated by factordb.".into(),
    ));
    module.add(Item::Comment("DO NOT EDIT MANUALLY".into()));
    module.add_newlines(1);

    let attr_name_constants = schema.attributes.iter().map(|attr| Item::Const {
        name: attr.ident.replace('/', "_").to_screaming_snake_case(),
        ty: None,
        value: Value::Str(attr.ident.clone()),
    });
    module.items.extend(attr_name_constants);
    module.add_newlines(1);

    // Base import or defintions.
    if let Some(source) = factor_import_path {
        let import = Item::Import {
            path: source.to_string(),
            items: vec![
                "EntityId".to_string(),
                "Url".to_string(),
                "Timestamp".to_string(),
                "BaseEntityData".to_string(),
            ],
        };
        module.add(import);
    } else {
        let id = Item::TypeAlias {
            name: "EntityId".to_string(),
            ty: Type::String,
        };
        let ident = Item::TypeAlias {
            name: "EntityIdent".to_string(),
            ty: Type::String,
        };
        let url = Item::TypeAlias {
            name: "Url".to_string(),
            ty: Type::String,
        };
        let timestamp = Item::TypeAlias {
            name: "Timestamp".to_string(),
            ty: Type::Number,
        };

        let id_or_ident = Item::TypeAlias {
            name: "IdOrIdent".to_string(),
            ty: Type::Union(vec![Type::Ident("EntityId".into()), Type::String]),
        };

        let base = Item::Interface {
            name: "BaseEntity".to_string(),
            extends: Vec::new(),
            ty: ObjectType {
                fields: vec![
                    FieldDef {
                        name: "factor/id".into(),
                        is_optional: false,
                        ty: Type::Ident("EntityId".into()),
                    },
                    FieldDef {
                        name: "factor/ident".into(),
                        is_optional: true,
                        ty: Type::Union(vec![Type::Ident("Ident".to_string()), Type::Null]),
                    },
                    FieldDef {
                        name: "factor/type".into(),
                        is_optional: true,
                        ty: Type::Union(vec![Type::String, Type::Null]),
                    },
                ],
            },
        };

        module
            .items
            .extend(vec![id, ident, id_or_ident, url, timestamp, base]);
    };

    module.add_newlines(1);

    let entities = schema
        .entities
        .iter()
        .map(|entity| build_entity(entity, schema))
        .collect::<Result<Vec<Vec<_>>, _>>()?
        .into_iter()
        .flatten();
    module.items.extend(entities);

    let code = module.render().trim().to_string();
    Ok(code)
}

fn build_entity(
    entity: &factordb::schema::EntitySchema,
    schema: &factordb::schema::DbSchema,
) -> Result<Vec<Item>, AnyError> {
    // find all the parent entities
    let parents = entity
        .extends
        .iter()
        .map(|ident| {
            schema
                .resolve_entity(ident)
                .ok_or_else(|| anyhow!("Parent entity {ident} not found"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let extends = if parents.is_empty() {
        vec!["BaseEntity".to_string()]
    } else {
        parents
            .iter()
            .map(|parent| -> String {
                let clean_name = parent.ident.replace('/', "_").to_class_case();

                format!("Omit<{}, \"factor/type\">", clean_name)
            })
            .collect::<Vec<_>>()
    };

    let fields = entity
        .attributes
        .iter()
        .map(|attr| -> Result<_, AnyError> {
            let s = schema
                .resolve_attr(&attr.attribute)
                .ok_or_else(|| anyhow!("Attribute {} not found", attr.attribute))?;
            Ok((attr.clone(), s.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut field_defs = Vec::new();
    let mut field_names = HashSet::new();
    for (field, attr) in &fields {
        // Guard against duplicate field names.
        // Note: this should not be necessary, but is here for now because of
        // some faulty schemas in older databases.
        if field_names.contains(&field.attribute) {
            continue;
        }
        field_names.insert(field.attribute.clone());

        if let Some(parent_attr) = schema.parent_entity_attr(&entity.ident(), &attr.ident()) {
            if parent_attr.cardinality == field.cardinality {
                continue;
            }
        }

        let ty = field_ts_type(field, attr);
        let def = FieldDef {
            name: attr.ident.clone(),
            is_optional: field.cardinality.is_optional(),
            ty,
        };

        field_defs.push(def);
    }

    let entity_name = entity.ident.replace('/', "_").to_class_case();

    field_defs.insert(
        0,
        FieldDef {
            name: "factor/type".to_string(),
            is_optional: false,
            ty: Type::Constant(Value::Str(entity.ident.clone())),
        },
    );

    let interface = Item::Interface {
        name: entity_name.clone(),
        extends,
        ty: ObjectType { fields: field_defs },
    };

    let ty_const = Item::Const {
        name: format!("TY_{}", entity_name.to_screaming_snake_case()),
        ty: None,
        value: Value::Str(entity.ident.clone()),
    };

    Ok(vec![ty_const, interface, Item::Newlines(1)])
}

#[allow(dead_code)]
#[derive(PartialEq, Eq, Debug)]
enum Value {
    Str(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl Value {
    fn render(self: &Value, indent: usize) -> String {
        let prefix: String = " ".repeat(indent);
        let out = match self {
            Value::Str(value) => format!("\"{}\"", value),
            Value::Array(items) => {
                let values = items
                    .iter()
                    .map(|item| item.render(indent + 2))
                    .collect::<Vec<_>>()
                    .join(",\n");
                format!("[\n{}\n{}]", values, prefix)
            }
            Value::Object(fields) => {
                let field_prefix: String = " ".repeat(indent + 2);
                let fields_rendered = fields
                    .iter()
                    .map(|(name, value)| {
                        let safe_name = make_save_ident(name);
                        let rendered_value = value.render(indent);
                        format!("{}{}: {},", field_prefix, safe_name, rendered_value)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!("{{\n{}{}}}", fields_rendered, prefix)
            }
        };
        format!("{}{}", prefix, out)
    }
}

#[derive(PartialEq, Eq, Debug)]
enum Type {
    Constant(Value),
    Any,
    Null,
    Void,
    Bool,
    Number,
    String,
    Array(Box<Self>),
    Object(ObjectType),
    Union(Vec<Self>),
    Ident(String),
    Generic { name: String, args: Vec<Self> },
}

impl Type {
    fn render(&self, indent: usize) -> String {
        match self {
            Type::Any => "any".to_string(),
            Type::Null => "null".to_string(),
            Type::Void => "void".to_string(),
            Type::Bool => "boolean".to_string(),
            Type::Number => "number".to_string(),
            Type::String => "string".to_string(),
            Type::Array(inner) => format!("{}[]", inner.render(indent)),
            Type::Object(obj) => {
                let fields = obj
                    .fields
                    .iter()
                    .map(|field| format!("{}: {},", field.name, field.ty.render(indent)))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{{{}}}", fields)
            }
            Type::Union(variants) => variants
                .iter()
                .map(|var| var.render(indent))
                .collect::<Vec<_>>()
                .join(" | "),
            Type::Ident(name) => name.clone(),
            Type::Generic { name, args } => {
                let generics = args
                    .iter()
                    .map(|g| g.render(indent))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}<{}>", name, generics)
            }
            Type::Constant(value) => value.render(indent),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
struct ObjectType {
    fields: Vec<FieldDef>,
}

#[derive(PartialEq, Eq, Debug)]
struct FieldDef {
    name: String,
    is_optional: bool,
    ty: Type,
}

#[derive(Debug)]
enum Item {
    Newlines(usize),
    TypeAlias {
        name: String,
        ty: Type,
    },
    Import {
        path: String,
        items: Vec<String>,
    },
    Comment(String),
    Const {
        name: String,
        ty: Option<Type>,
        value: Value,
    },
    Interface {
        name: String,
        extends: Vec<String>,
        ty: ObjectType,
    },
}

impl Item {
    fn render(&self) -> String {
        match self {
            Item::Import { path, items } => {
                let rendered_items = items.join(", ");
                format!("import {{{}}} from \"{}\";", rendered_items, path)
            }
            Item::Comment(txt) => format!("// {}", txt),
            Item::Const { name, ty, value } => {
                let ty_rendered = if let Some(ty) = ty {
                    format!(": {}", ty.render(0))
                } else {
                    String::new()
                };
                format!(
                    "export const {}{} = {};",
                    name,
                    ty_rendered,
                    value.render(0),
                )
            }
            Item::Interface {
                name,
                extends,
                ty: ObjectType { fields },
            } => {
                let extends_rendered = if extends.is_empty() {
                    String::new()
                } else {
                    let items = extends.join(", ");
                    format!(" extends {}", items)
                };
                let fields_rendered: String = fields
                    .iter()
                    .map(|field| {
                        let safe_name = make_save_ident(&field.name);
                        let opt = if field.is_optional { "?" } else { "" };
                        format!("  {}{}: {},", safe_name, opt, field.ty.render(0))
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!(
                    "export interface {}{} {{\n{}\n}}",
                    name, extends_rendered, fields_rendered
                )
            }
            Item::TypeAlias { name, ty } => {
                format!("export type {} = {};", name, ty.render(0))
            }
            Item::Newlines(count) => "\n".repeat(*count),
        }
    }
}

#[derive(Debug)]
struct Module {
    items: Vec<Item>,
}

impl Module {
    fn add(&mut self, item: Item) {
        self.items.push(item);
    }

    fn add_newlines(&mut self, count: usize) {
        self.items.push(Item::Newlines(count));
    }

    fn render(&self) -> String {
        self.items
            .iter()
            .map(|item| {
                let mut code = item.render();
                code.push('\n');
                code
            })
            .collect()
    }
}

fn is_save_ident(value: &str) -> bool {
    value
        .chars()
        .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '_'))
}

fn make_save_ident(name: &str) -> String {
    if is_save_ident(name) {
        name.to_string()
    } else {
        format!("\"{}\"", name)
    }
}

fn field_ts_type(
    field: &factordb::schema::EntityAttribute,
    attr: &factordb::schema::AttributeSchema,
) -> Type {
    let inner = value_to_ts_type(&attr.value_type);
    match field.cardinality {
        factordb::schema::Cardinality::Optional => Type::Union(vec![inner, Type::Null]),
        factordb::schema::Cardinality::Required => inner,
    }
}

fn value_to_ts_type(ty: &ValueType) -> Type {
    match ty {
        ValueType::Any => Type::Any,
        ValueType::Unit => Type::Void,
        ValueType::Bool => Type::Bool,
        ValueType::Int | ValueType::UInt | ValueType::Float => Type::Number,
        ValueType::String => Type::String,
        // TODO: how to represent byte arrays?
        ValueType::Bytes => Type::Array(Box::new(Type::Number)),
        ValueType::List(inner) => Type::Array(Box::new(value_to_ts_type(inner))),
        ValueType::Map(ty) => Type::Generic {
            name: "Record".to_string(),
            args: vec![value_to_ts_type(&ty.key), value_to_ts_type(&ty.value)],
        },
        ValueType::Union(variants) => {
            let vars = variants.iter().map(value_to_ts_type).collect::<Vec<_>>();
            Type::Union(vars)
        }
        ValueType::Object(obj) => {
            let fields = obj
                .fields
                .iter()
                .map(|field| FieldDef {
                    name: field.name.clone(),
                    is_optional: false,
                    ty: value_to_ts_type(&field.value_type),
                })
                .collect::<Vec<_>>();
            Type::Object(ObjectType { fields })
        }
        ValueType::DateTime => Type::Ident("Timestamp".to_string()),
        ValueType::Url => Type::Ident("Url".to_string()),
        ValueType::Ref => Type::Ident("EntityId".to_string()),
        ValueType::Ident(_) => Type::Ident("EntityIdent".to_string()),
        ValueType::RefConstrained(_) => {
            // TODO: use type alias for specific entity id if restricted to single type
            Type::Ident("EntityId".to_string())
        }
        ValueType::Const(v) => Type::Constant(value_to_ts_value(v)),
    }
}

fn value_to_ts_value(v: &factordb::data::Value) -> Value {
    match v {
        factordb::data::Value::Unit => todo!(),
        factordb::data::Value::Bool(_) => todo!(),
        factordb::data::Value::UInt(_) => todo!(),
        factordb::data::Value::Int(_) => todo!(),
        factordb::data::Value::Float(_) => todo!(),
        factordb::data::Value::String(s) => Value::Str(s.clone()),
        factordb::data::Value::Bytes(_) => todo!(),
        factordb::data::Value::List(_) => todo!(),
        factordb::data::Value::Map(_) => todo!(),
        factordb::data::Value::Id(_) => todo!(),
    }
}
