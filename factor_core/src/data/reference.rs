use std::{borrow::Cow, str::FromStr};

use super::{id::CowStr, DataMap, Id};

/// A reference to another entity.
///
/// May be either a unique id, ident or a nested entity.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[serde(untagged)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
pub enum Ref<T = DataMap> {
    Id(Id),
    Name(CowStr),
    Nested(T),
}

#[cfg(feature = "typescript-schema")]
impl<T> ts_rs::TS for Ref<T>
where
    T: ts_rs::TS,
{
    fn name() -> String {
        "Ref".to_string()
    }

    fn name_with_type_args(args: Vec<String>) -> String {
        if args.is_empty() {
            "Ref".to_string()
        } else {
            format!("Ref<{}>", args.join(","))
        }
    }

    fn decl() -> String {
        "type Ref<T> = Id | string | T;".to_string()
    }

    fn inline() -> String {
        "Id | string | T".to_string()
    }

    fn dependencies() -> Vec<ts_rs::Dependency> {
        vec![]
    }

    fn transparent() -> bool {
        false
    }
}

impl<T> Ref<T> {
    pub const fn new_static(value: &'static str) -> Self {
        Self::Name(CowStr::Borrowed(value))
    }

    pub fn new_nested(value: T) -> Self {
        Self::Nested(value)
    }

    pub fn new_str(value: &str) -> Self {
        if let Ok(id) = uuid::Uuid::from_str(value) {
            Self::Id(super::id::Id(id))
        } else {
            Self::Name(value.to_string().into())
        }
    }

    /// Returns `true` if the ident is [`Id`].
    pub fn is_id(&self) -> bool {
        matches!(self, Self::Id(..))
    }

    /// Returns `true` if the ident is [`Name`].
    pub fn is_name(&self) -> bool {
        matches!(self, Self::Name(..))
    }
}

impl<T> From<Id> for Ref<T> {
    fn from(id: Id) -> Self {
        Self::Id(id)
    }
}

impl<T> From<String> for Ref<T> {
    fn from(v: String) -> Self {
        Self::Name(CowStr::from(v))
    }
}

impl<'a, T> From<&'a str> for Ref<T> {
    fn from(v: &'a str) -> Self {
        Self::Name(Cow::from(v.to_string()))
    }
}
