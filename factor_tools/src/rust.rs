use std::collections::HashSet;
use std::fmt::Write;
use std::{collections::HashMap, path::PathBuf};

use anyhow::Context;
use factor_core::{
    data::{from_value_map, ValueType},
    schema::{
        builtin::AttrIdent, AttrMapExt, Attribute, AttributeMeta, Class, ClassMeta, StaticSchema,
    },
    simple_db::SimpleDb,
};
use inflector::Inflector;

pub struct RustAttribute {
    pub name: String,
    pub value: Option<String>,
}

impl RustAttribute {
    pub fn render(&self) -> String {
        if let Some(value) = &self.value {
            format!("#[{}({})]", self.name, value)
        } else {
            format!("#[{}]", self.name)
        }
    }
}

impl RustAttribute {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
        }
    }
}

fn render_attrs(attrs: &[RustAttribute], indent: usize) -> String {
    if !attrs.is_empty() {
        let mut s = attrs
            .iter()
            .map(|a| {
                let r = a.render();
                let indent = " ".repeat(indent);
                format!("{}{}", indent, r)
            })
            .collect::<Vec<_>>()
            .join("\n");
        s.push('\n');
        s
    } else {
        String::new()
    }
}

pub struct TupleField {
    // attributes: Vec<RustAttribute>,
    ty: String,
}

impl TupleField {
    pub fn render(&self) -> String {
        format!("pub {}", self.ty)
    }
}

pub struct NamedField {
    pub attributes: Vec<RustAttribute>,
    pub name: String,
    pub ty: String,
}

impl NamedField {
    pub fn render(&self) -> String {
        let attrs = render_attrs(&self.attributes, 4);
        format!("{attrs}    {}: {}", self.name, self.ty)
    }
}

pub enum StructFields {
    None,
    Tuple(Vec<TupleField>),
    Named(Vec<NamedField>),
}

pub struct RustStruct {
    pub name: String,
    pub derives: Vec<String>,
    pub attributes: Vec<RustAttribute>,
    pub fields: StructFields,
}

impl RustStruct {
    pub fn render(&self) -> String {
        let mut s = String::new();

        if !self.derives.is_empty() {
            s.push_str(&render_attrs(
                &[RustAttribute::new("derive", self.derives.join(", "))],
                0,
            ));
        }

        s.push_str(&render_attrs(&self.attributes, 0));

        write!(&mut s, "pub struct {}", self.name).unwrap();

        match &self.fields {
            StructFields::None => {
                s.push_str(";\n");
            }
            StructFields::Tuple(tuple) => {
                let inner = tuple
                    .iter()
                    .map(|field| field.render())
                    .collect::<Vec<_>>()
                    .join(", ");
                writeln!(&mut s, "({});\n", inner).unwrap();
            }
            StructFields::Named(named) => {
                if !named.is_empty() {
                    let inner = named
                        .iter()
                        .map(|field| field.render())
                        .collect::<Vec<_>>()
                        .join(",\n");
                    writeln!(&mut s, " {{\n{inner}\n}}").unwrap();
                } else {
                    s.push_str("{};\n");
                }
            }
        }
        s
    }
}

pub struct RustArg {
    pub name: String,
    pub ty: String,
}

pub struct RustFunc {
    pub name: String,
    pub args: Vec<RustArg>,
    pub return_type: String,
    pub body: String,
}

impl RustFunc {
    pub fn render(&self) -> String {
        let args = self
            .args
            .iter()
            .map(|arg| format!("{}: {}", arg.name, arg.ty))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "pub fn {}({}) -> {} {{\n{}\n}}\n",
            self.name,
            args,
            self.return_type,
            indent(&self.body, 4)
        )
    }
}

pub struct RustConst {
    pub name: String,
    pub ty: String,
    pub value: Expr,
}

impl RustConst {
    pub fn render(&self) -> String {
        format!(
            "pub const {}: {} = {};\n",
            self.name,
            self.ty,
            self.value.render()
        )
    }
}

pub struct ImplType {
    pub name: String,
    pub value: String,
}

impl ImplType {
    pub fn render(&self) -> String {
        format!("type {} = {};\n", self.name, self.value)
    }
}

fn indent(s: &str, indent: usize) -> String {
    let prefix = " ".repeat(indent);
    s.lines()
        .map(|line| format!("{}{}", prefix, line))
        .collect::<Vec<_>>()
        .join("\n")
}

pub enum RustImplItem {
    Func(RustFunc),
    Const(RustConst),
    Type(ImplType),
}

impl RustImplItem {
    pub fn render(&self) -> String {
        match self {
            Self::Func(v) => indent(&v.render(), 4),
            Self::Const(v) => indent(&v.render(), 4),
            Self::Type(v) => indent(&v.render(), 4),
        }
    }
}

pub struct RustImpl {
    pub trait_name: String,
    pub type_name: String,
    pub items: Vec<RustImplItem>,
}

impl RustImpl {
    pub fn render(&self) -> String {
        let items = self
            .items
            .iter()
            .map(|f| f.render())
            .collect::<Vec<_>>()
            .join("\n");
        let s = format!(
            "impl {} for {} {{\n{}\n}}\n",
            self.trait_name, self.type_name, items
        );
        s
    }
}

pub struct Call {
    pub target: Expr,
    pub args: Vec<Expr>,
}

impl Call {
    pub fn render(&self) -> String {
        let args = self
            .args
            .iter()
            .map(|arg| arg.render())
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}({})", self.target.render(), args)
    }
}

pub enum Expr {
    Int(i128),
    Float(f64),
    Bool(bool),
    Str(String),
    Ident(String),
    Struct(StructLiteral),
    Call(Box<Call>),

    Other(String),
}

impl Expr {
    fn str(value: &str) -> Self {
        Self::Str(value.to_string())
    }

    pub fn ident(value: impl Into<String>) -> Self {
        Self::Ident(value.into())
    }

    pub fn other(value: impl Into<String>) -> Self {
        Self::Other(value.into())
    }

    pub fn render(&self) -> String {
        match self {
            Self::Int(v) => format!("{}", v),
            Self::Float(v) => format!("{}", v),
            Self::Bool(v) => {
                if *v {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            Self::Str(v) => format!("\"{}\"", v),
            Self::Ident(v) => v.clone(),
            Self::Struct(v) => v.render(),
            Self::Call(v) => v.render(),
            Self::Other(v) => v.clone(),
        }
    }
}

pub struct StructLiteral {
    pub name: String,
    pub fields: Vec<(String, Expr)>,
}

impl StructLiteral {
    pub fn render(&self) -> String {
        let fields = self
            .fields
            .iter()
            .map(|(name, value)| format!("    {}: {}", name, value.render()))
            .collect::<Vec<_>>()
            .join(",\n");
        format!("{} {{\n{}\n}}\n", self.name, fields)
    }
}

pub enum Item {
    Struct(RustStruct),
    Impl(RustImpl),
}

impl Item {
    pub fn render(&self) -> String {
        match self {
            Item::Struct(s) => s.render(),
            Item::Impl(i) => i.render(),
        }
    }
}

#[derive(Default)]
pub struct Module {
    pub items: Vec<Item>,
}

impl Module {
    pub fn render(&self) -> String {
        self.items
            .iter()
            .map(|i| i.render())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Default)]
pub struct Schema {
    pub attributes: HashMap<String, Attribute>,
    pub classes: HashMap<String, Class>,
    pub external: HashSet<String>,
}

fn value_type_to_rust_type(value_type: &ValueType, schema: &Schema) -> String {
    match value_type {
        ValueType::Any => todo!(),
        ValueType::Unit => todo!(),
        ValueType::Bool => "bool".to_string(),
        ValueType::Int => "i64".to_string(),
        ValueType::UInt => "u64".to_string(),
        ValueType::Float => "f64".to_string(),
        ValueType::String => "String".to_string(),
        ValueType::Bytes => "Vec<u8>".to_string(),
        ValueType::List(inner) => {
            let inner_type = value_type_to_rust_type(inner, schema);
            format!("Vec<{}>", inner_type)
        }
        ValueType::Map(map) => {
            let key_type = value_type_to_rust_type(&map.key, schema);
            let value_type = value_type_to_rust_type(&map.value, schema);
            format!("std::collections::HashMap<{}, {}>", key_type, value_type)
        }
        ValueType::Union(_) => todo!(),
        ValueType::Object(_) => todo!(),
        ValueType::DateTime => "factdb::Timestamp".to_string(),
        ValueType::Url => "url::Url".to_string(),
        ValueType::Ref => "String".to_string(),
        ValueType::Ident(constraints) => todo!(),
        ValueType::RefConstrained(constraints) => todo!(),
        ValueType::EmbeddedEntity => "factdb::DataMap".to_string(),
        ValueType::Const(_) => todo!(),
    }
}

fn value_type_to_expr(ty: &ValueType) -> Expr {
    match ty {
        ValueType::Any => todo!(),
        ValueType::Unit => todo!(),
        ValueType::Bool => Expr::other("factdb::ValueType::Bool"),
        ValueType::Int => Expr::other("factdb::ValueType::Int"),
        ValueType::UInt => Expr::other("factdb::ValueType::UInt"),
        ValueType::Float => Expr::other("factdb::ValueType::Float"),
        ValueType::String => Expr::other("factdb::ValueType::String"),
        ValueType::Bytes => Expr::other("Vec<u8>"),
        ValueType::List(inner) => Expr::Other(format!(
            "factdb::ValueType::List(Box::new({}))",
            value_type_to_expr(inner).render(),
        )),
        ValueType::Map(_) => todo!(),
        ValueType::Union(_) => todo!(),
        ValueType::Object(_) => todo!(),
        ValueType::DateTime => Expr::other("factdb::ValueType::DateTime"),
        ValueType::Url => Expr::other("factdb::ValueType::Url"),
        ValueType::Ref => Expr::other("factdb::ValueType::Ref"),
        ValueType::Ident(_) => todo!(),
        ValueType::RefConstrained(_) => todo!(),
        ValueType::EmbeddedEntity => todo!(),
        ValueType::Const(_) => todo!(),
    }
}

pub fn generate_schema(
    schema: &StaticSchema,
    with_builtins: bool,
) -> Result<String, anyhow::Error> {
    let mut db = SimpleDb::new();

    for migration in &schema.migrations {
        for commit in &migration.commits {
            db = db.apply_pre_commit(commit.clone())?;
        }
    }
    let mut schema = Schema::default();

    for raw_attr in db.entities_by_type(Attribute::QUALIFIED_NAME) {
        let id = raw_attr.get_id().unwrap();
        let ident = raw_attr
            .get(AttrIdent::QUALIFIED_NAME)
            .and_then(|x| x.as_str())
            .with_context(|| format!("Invalid attribute with id '{id}': attribtue has no ident!"))?
            .to_string();

        let attr: Attribute = from_value_map(raw_attr.clone())
            .with_context(|| format!("Invalid attribute '{ident}'"))?;
        schema.attributes.insert(ident, attr);
    }

    for raw_class in db.entities_by_type(Class::QUALIFIED_NAME) {
        let id = raw_class.get_id().unwrap();
        let ident = raw_class
            .get(AttrIdent::QUALIFIED_NAME)
            .and_then(|x| x.as_str())
            .with_context(|| format!("Invalid class with id '{id}': class has no ident!"))?
            .to_string();

        let class: Class = from_value_map(raw_class.clone())
            .with_context(|| format!("Invalid class '{ident}'"))?;
        schema.classes.insert(ident, class);
    }

    if with_builtins {
        let builtins = factor_core::schema::builtin::builtin_db_schema();
        for attr in builtins.attributes {
            schema.external.insert(attr.ident.clone());
            schema.attributes.insert(attr.ident.clone(), attr);
        }
        for class in builtins.classes {
            schema.external.insert(class.ident.clone());
            schema.classes.insert(class.ident.clone(), class);
        }
    }

    let mut module = Module::default();

    for attr in schema
        .attributes
        .values()
        .filter(|a| !schema.external.contains(&a.ident))
    {
        let (namespace, plain_name) = attr.parse_split_ident().unwrap();
        let type_name = format!("Attr{}", plain_name.to_pascal_case());
        let rust_type = value_type_to_rust_type(&attr.value_type, &schema);

        let s = RustStruct {
            name: type_name.clone(),
            derives: vec![
                "serde_derive::Serialize".to_string(),
                "serde_derive::Deserialize".to_string(),
                "Clone".to_string(),
                "Debug".to_string(),
            ],
            attributes: vec![],
            fields: StructFields::Tuple(vec![TupleField {
                ty: rust_type.clone(),
                // attributes: vec![],
            }]),
        };

        let impl_ = RustImpl {
            trait_name: "factdb::AttributeMeta".to_string(),
            type_name,
            items: vec![
                RustImplItem::Const(RustConst {
                    name: "NAMESPACE".to_string(),
                    ty: "&'static str".to_string(),
                    value: Expr::str(namespace),
                }),
                RustImplItem::Const(RustConst {
                    name: "PLAIN_NAME".to_string(),
                    ty: "&'static str".to_string(),
                    value: Expr::str(plain_name),
                }),
                RustImplItem::Const(RustConst {
                    name: "QUALIFIED_NAME".to_string(),
                    ty: "&'static str".to_string(),
                    value: Expr::str(&attr.ident),
                }),
                RustImplItem::Const(RustConst {
                    name: "IDENT".to_string(),
                    ty: "factdb::IdOrIdent".to_string(),
                    value: Expr::other(
                        "factdb::IdOrIdent::new_static(Self::QUALIFIED_NAME)".to_string(),
                    ),
                }),
                RustImplItem::Type(ImplType {
                    name: "Type".to_string(),
                    value: rust_type,
                }),
                RustImplItem::Func(RustFunc {
                    name: "schema".to_string(),
                    args: vec![],
                    return_type: "factdb::Attribute".to_string(),
                    body: StructLiteral {
                        name: "factdb::Attribute".to_string(),
                        fields: vec![
                            (
                                "id".to_string(),
                                Expr::Other("factdb::Id::nil()".to_string()),
                            ),
                            (
                                "ident".to_string(),
                                Expr::Other("Self::QUALIFIED_NAME.to_string()".to_string()),
                            ),
                            // TODO: title + description
                            ("title".to_string(), Expr::Other("None".to_string())),
                            ("description".to_string(), Expr::Other("None".to_string())),
                            (
                                "value_type".to_string(),
                                value_type_to_expr(&attr.value_type),
                            ),
                            ("unique".to_string(), Expr::Bool(attr.unique)),
                            ("index".to_string(), Expr::Bool(attr.index)),
                            ("strict".to_string(), Expr::Bool(attr.strict)),
                        ],
                    }
                    .render(),
                }),
            ],
        };

        module.items.push(Item::Struct(s));
        module.items.push(Item::Impl(impl_));
    }

    for class in schema
        .classes
        .values()
        .filter(|c| !schema.external.contains(&c.ident))
    {
        let (namespace, plain_name) = class.parse_split_ident().unwrap();
        let class_type_name = plain_name.to_pascal_case();

        let mut fields = Vec::new();

        fields.push(NamedField {
            attributes: vec![RustAttribute::new(
                "serde",
                "rename = \"factor/id\", default",
            )],
            name: "id".to_string(),
            ty: "factdb::Id".to_string(),
        });

        for parent_ident in &class.extends {
            let parent_class = schema.classes.get(parent_ident).with_context(|| {
                format!(
                    "Invalid class '{}': parent '{}' not found",
                    class.ident, parent_ident
                )
            })?;
            let parent_name = parent_class.parse_split_ident().unwrap().1.to_snake_case();
            let parent_type = parent_class.parse_split_ident().unwrap().1.to_pascal_case();

            fields.push(NamedField {
                attributes: vec![RustAttribute::new("serde", "flatten")],
                name: parent_name.to_string(),
                ty: parent_type,
            })
        }

        for field in &class.attributes {
            let attr_name = &field.attribute;
            let attr = schema.attributes.get(attr_name).with_context(|| {
                format!(
                    "Invalid class '{}': attribute '{}' not found",
                    class.ident, attr_name
                )
            })?;

            let field_name = attr_name.to_snake_case();
            let ty = value_type_to_rust_type(&attr.value_type, &schema);

            fields.push(NamedField {
                attributes: vec![],
                name: field_name,
                ty,
            });
        }

        if !class.strict {
            fields.push(NamedField {
                attributes: vec![RustAttribute::new("serde", "flatten")],
                name: "extra".to_string(),
                ty: "factdb::DataMap".to_string(),
            });
        }

        let s = RustStruct {
            name: class_type_name.clone(),
            derives: vec![
                "serde_derive::Serialize".to_string(),
                "serde_derive::Deserialize".to_string(),
                "Clone".to_string(),
                "Debug".to_string(),
            ],
            attributes: vec![],
            fields: StructFields::Named(fields),
        };

        let impl_ = RustImpl {
            trait_name: "factdb::ClassMeta".to_string(),
            type_name: class_type_name,
            items: vec![
                RustImplItem::Const(RustConst {
                    name: "NAMESPACE".to_string(),
                    ty: "&'static str".to_string(),
                    value: Expr::str(namespace),
                }),
                RustImplItem::Const(RustConst {
                    name: "PLAIN_NAME".to_string(),
                    ty: "&'static str".to_string(),
                    value: Expr::str(plain_name),
                }),
                RustImplItem::Const(RustConst {
                    name: "QUALIFIED_NAME".to_string(),
                    ty: "&'static str".to_string(),
                    value: Expr::str(&class.ident),
                }),
                RustImplItem::Const(RustConst {
                    name: "IDENT".to_string(),
                    ty: "factdb::IdOrIdent".to_string(),
                    value: Expr::other("factdb::IdOrIdent::new_static(Self::QUALIFIED_NAME)"),
                }),
                RustImplItem::Func(RustFunc {
                    name: "schema".to_string(),
                    args: vec![],
                    return_type: "factdb::Class".to_string(),
                    body: StructLiteral {
                        name: "factdb::Class".to_string(),
                        fields: vec![
                            (
                                "id".to_string(),
                                Expr::Other("factdb::Id::nil()".to_string()),
                            ),
                            (
                                "ident".to_string(),
                                Expr::other("Self::QUALIFIED_NAME.to_string()"),
                            ),
                            // TODO: title + description
                            ("title".to_string(), Expr::Other("None".to_string())),
                            ("description".to_string(), Expr::Other("None".to_string())),
                            ("strict".to_string(), Expr::Bool(class.strict)),
                            (
                                "extends".to_string(),
                                Expr::Other(format!(
                                    "vec![{}]",
                                    class
                                        .extends
                                        .iter()
                                        .map(|x| format!("\"{}\".to_string()", x))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )),
                            ),
                            (
                                "attributes".to_string(),
                                Expr::Other(format!(
                                    "vec![{}]",
                                    class
                                        .attributes
                                        .iter()
                                        .map(|attr| StructLiteral {
                                            name: "factdb::ClassAttribute".to_string(),
                                            fields: vec![
                                                (
                                                    "attribute".to_string(),
                                                    Expr::other(format!(
                                                        "\"{}\".to_string()",
                                                        attr.attribute
                                                    )),
                                                ),
                                                ("required".to_string(), Expr::Bool(attr.required)),
                                            ],
                                        }
                                        .render())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )),
                            ),
                        ],
                    }
                    .render(),
                }),
            ],
        };

        module.items.push(Item::Struct(s));
        module.items.push(Item::Impl(impl_))
    }

    let content = module.render();
    let code = format!(
        "// AUTO-GENERATED FILE. DO NOT EDIT MANUALLY!\n\n{}\n",
        content
    );

    Ok(code)
}

pub fn generate_schema_from_json(
    contents: &str,
    with_builtins: bool,
) -> Result<String, anyhow::Error> {
    let jd = &mut serde_json::Deserializer::from_str(contents);
    let schema: StaticSchema = serde_path_to_error::deserialize(jd)?;
    generate_schema(&schema, with_builtins)
}

pub fn generate_schema_from_file(
    path: impl Into<PathBuf>,
    with_builtins: bool,
) -> Result<String, anyhow::Error> {
    let path = path.into();
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Could not read file '{}'", path.display()))?;
    let schema: StaticSchema = serde_json::from_str(&contents)?;
    generate_schema(&schema, with_builtins)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_schema_rust_codegen() {
        let schema = r#"
{
"factor/ident": "TestSchema",
"factor/migrations": [
{
"factor/commits": [
    {
        "factor/subject": "test/MyAttr",
        "factor/set": {
            "factor/type": "factor/Attribute",
            "factor/valueType": "String"
        }
    },
    {
        "factor/subject": "test/MyClass",
        "factor/set": {
            "factor/type": "factor/Class",
            "factor/entityAttributes": [
            ]
        }
    },
    {
        "factor/subject": "test/MyChildClass",
        "factor/set": {
            "factor/type": "factor/Class",
            "factor/entityAttributes": [
                {
                    "factor/attribute": "factor/title",
                    "factor/required": true
                }
            ],
            "factor/extend": [
                "test/MyClass"
            ]
        }
    }
]
}
]
}
"#;
        let code = generate_schema_from_json(schema, true).unwrap();
        eprintln!("{code}");
    }
}
