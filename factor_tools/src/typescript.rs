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
        let url = Item::TypeAlias {
            name: "Url".to_string(),
            ty: Type::String,
        };
        let timestamp = Item::TypeAlias {
            name: "Timestamp".to_string(),
            ty: Type::Number,
        };

        let ident = Item::TypeAlias {
            name: "Ident".to_string(),
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

        module.items.extend(vec![id, ident, url, timestamp, base]);
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
    let extends = if entity.extends.is_empty() {
        vec!["BaseEntity".to_string()]
    } else {
        entity
            .extends
            .iter()
            .map(|parent_ident| -> Result<_, AnyError> {
                let parent = schema
                    .resolve_entity(parent_ident)
                    .ok_or_else(|| anyhow!("Could not find entity '{:?}'", parent_ident))?;
                let clean_name = parent.ident.replace('/', "_").to_class_case();

                Ok(format!("Omit<{}, \"factor/type\">", clean_name))
            })
            .collect::<Result<_, _>>()?
    };

    let mut fields = entity
        .attributes
        .iter()
        .map(|field| -> Result<_, AnyError> {
            // TODO: extract object types into separate definitions.

            let attr = schema.resolve_attr(&field.attribute).ok_or_else(|| {
                anyhow!("Could not find attribute {:?} in schema", field.attribute)
            })?;

            let ty = field_ts_type(field, attr);

            Ok(FieldDef {
                name: attr.ident.clone(),
                is_optional: field.cardinality.is_optional(),
                ty,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let entity_name = entity.ident.replace('/', "_").to_class_case();

    fields.insert(
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
        ty: ObjectType { fields },
    };

    let ty_const = Item::Const {
        name: format!("TY_{}", entity_name.to_screaming_snake_case()),
        ty: None,
        value: Value::Str(entity.ident.clone()),
    };

    Ok(vec![ty_const, interface, Item::Newlines(1)])
}
enum Value {
    Str(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl Value {
    fn render(self: &Value, indent: usize) -> String {
        let prefix: String = std::iter::repeat(' ').take(indent).collect();
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
                let field_prefix: String = std::iter::repeat(' ').take(indent + 2).collect();
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

struct ObjectType {
    fields: Vec<FieldDef>,
}

struct FieldDef {
    name: String,
    is_optional: bool,
    ty: Type,
}

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
            Item::Newlines(count) => std::iter::repeat('\n').take(*count).collect(),
        }
    }
}

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
                code.push_str("\n");
                code
            })
            .collect()
    }
}

fn is_save_ident(value: &str) -> bool {
    value.chars().all(|c| match c {
        'a'..='z' | 'A'..='Z' | '_' => true,
        _ => false,
    })
}

fn make_save_ident(name: &str) -> String {
    if is_save_ident(&name) {
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
        factordb::schema::Cardinality::Many => Type::Array(Box::new(inner)),
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
        ValueType::Map => Type::Generic {
            name: "Record".to_string(),
            args: vec![Type::String, Type::Any],
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