use std::convert::TryFrom;

use crate::{
    data::{Id, Ident, ValueType},
    Value, ValueMap,
};

use super::EntityContainer;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeSchema {
    #[serde(rename = "factor/id")]
    pub id: Id,
    #[serde(rename = "factor/ident")]
    pub ident: String,
    #[serde(rename = "factor/title")]
    pub title: Option<String>,
    #[serde(rename = "factor/description")]
    pub description: Option<String>,
    #[serde(rename = "factor/valueType")]
    pub value_type: ValueType,
    #[serde(rename = "factor/unique")]
    pub unique: bool,
    #[serde(rename = "factor/index")]
    pub index: bool,
    /// If an attribute is set to strict, this attribute can only be used
    /// in entities with a schema that specifies the attribute.
    #[serde(rename = "factor/isStrict")]
    pub strict: bool,
}

impl AttributeSchema {
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        value_type: ValueType,
    ) -> Self {
        Self {
            id: Id::nil(),
            ident: format!("{}/{}", namespace.into(), name.into()),
            title: None,
            description: None,
            value_type,
            unique: false,
            index: false,
            strict: false,
        }
    }

    pub fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    pub fn with_indexed(mut self, index: bool) -> Self {
        self.index = index;
        self
    }

    /// Split the ident into (namespace, name)
    pub fn parse_split_ident(&self) -> Result<(&str, &str), crate::AnyError> {
        super::validate_namespaced_ident(&self.ident)
    }

    pub fn parse_namespace(&self) -> Result<&str, crate::AnyError> {
        self.parse_split_ident().map(|x| x.0)
    }
}

/// A marker trait for attributes.
///
/// Makes working with statically typed attributes in Rust code easier.
///
/// Useful for defining attributes in migrations, or getting attribute values
/// from a value map with [AttrMapExt].
///
/// NOTE: Types implementing this trait won't usually be used to represent
/// attributes, but act merely as a descriptor.
///
/// This trait should generally not be implemented manually.
/// A custom derive proc macro is available.
/// See [`crate::Attribute`] for how to use the derive.
pub trait AttributeDescriptor {
    /// The namespace fo the attribute.
    const NAMESPACE: &'static str;
    /// The name of the attribute.
    const PLAIN_NAME: &'static str;
    /// The qualified name of the attribute.
    /// This MUST be equal to `format!("{}/{}", Self::NAMESPACE, Self::NAME)`.
    /// Only exists to not require string allocation and concatenation at
    /// runtime.
    const QUALIFIED_NAME: &'static str;
    const IDENT: Ident = Ident::new_static(Self::QUALIFIED_NAME);

    /// The Rust type used to represent this attribute.
    type Type;

    /// Build the schema for this attribute.
    fn schema() -> AttributeSchema;

    /// Build an expression that selects this attribute.
    fn expr() -> crate::query::expr::Expr {
        crate::query::expr::Expr::Attr(Self::QUALIFIED_NAME.into())
    }
}

pub trait AttrMapExt {
    fn get_id(&self) -> Option<Id>;
    fn get_ident(&self) -> Option<Ident>;

    fn get_type(&self) -> Option<Ident>;
    fn get_type_name(&self) -> Option<&str>;

    fn has_attr<A: AttributeDescriptor>(&self) -> bool;
    fn get_attr<A: AttributeDescriptor>(&self) -> Option<A::Type>
    where
        A::Type: TryFrom<Value>;

    fn get_attr_vec<A: AttributeDescriptor>(&self) -> Option<Vec<A::Type>>
    where
        A::Type: TryFrom<Value>;

    fn insert_attr<A: AttributeDescriptor>(&mut self, value: A::Type)
    where
        A::Type: Into<Value>;

    fn try_into_entity<E>(self) -> Result<E, crate::data::value::ValueDeserializeError>
    where
        Self: Sized,
        E: EntityContainer + serde::de::DeserializeOwned;
}

impl AttrMapExt for ValueMap<String> {
    fn get_id(&self) -> Option<Id> {
        self.get(super::builtin::AttrId::QUALIFIED_NAME)
            .and_then(|v| v.as_id())
    }

    fn get_ident(&self) -> Option<Ident> {
        self.get_id().map(Ident::from).or_else(|| {
            self.get_attr::<super::builtin::AttrIdent>()
                .map(|s| s.into())
        })
    }

    fn get_type(&self) -> Option<Ident> {
        self.get(super::builtin::AttrType::QUALIFIED_NAME)
            .and_then(|v| match v {
                Value::String(name) => Some(Ident::Name(name.to_string().into())),
                Value::Id(id) => Some(Ident::Id(*id)),
                _ => None,
            })
    }

    fn get_type_name(&self) -> Option<&str> {
        self.get(super::builtin::AttrType::QUALIFIED_NAME)
            .and_then(|v| match v {
                Value::String(name) => Some(name.as_str()),
                _ => None,
            })
    }

    fn has_attr<A: AttributeDescriptor>(&self) -> bool {
        self.0.contains_key(A::QUALIFIED_NAME)
    }

    fn get_attr<A: AttributeDescriptor>(&self) -> Option<A::Type>
    where
        A::Type: TryFrom<Value>,
    {
        let value = self.get(A::QUALIFIED_NAME)?.clone();
        TryFrom::try_from(value).ok()
    }

    fn get_attr_vec<A: AttributeDescriptor>(&self) -> Option<Vec<A::Type>>
    where
        A::Type: TryFrom<Value>,
    {
        match self.get(A::QUALIFIED_NAME)? {
            Value::List(items) => {
                let mut typed = Vec::new();
                for item in items {
                    let t: A::Type = TryFrom::try_from(item.clone()).ok()?;
                    typed.push(t);
                }
                Some(typed)
            }
            _ => None,
        }
    }

    fn insert_attr<A: AttributeDescriptor>(&mut self, value: A::Type)
    where
        A::Type: Into<Value>,
    {
        self.insert(A::QUALIFIED_NAME.to_string(), value.into());
    }

    fn try_into_entity<E>(self) -> Result<E, crate::data::value::ValueDeserializeError>
    where
        Self: Sized,
        E: EntityContainer + serde::de::DeserializeOwned,
    {
        crate::data::value::from_value_map(self)
    }
}
