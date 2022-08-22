use std::{borrow::Cow, str::FromStr};

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Id(pub uuid::Uuid);

impl Default for Id {
    fn default() -> Self {
        Self::nil()
    }
}

impl From<uuid::Uuid> for Id {
    fn from(id: uuid::Uuid) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl super::value_type::ValueTypeDescriptor for Id {
    fn value_type() -> super::ValueType {
        super::ValueType::Ref
    }
}

impl FromStr for Id {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<uuid::Uuid>().map(Self)
    }
}

impl Id {
    pub const fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }

    /// Either returns the id if it is not nil, or otherwise creates a new
    /// random one.
    pub fn non_nil_or_randomize(self) -> Self {
        if self.is_nil() {
            Self::random()
        } else {
            self
        }
    }

    pub(crate) const fn from_u128(value: u128) -> Self {
        Self(uuid::Uuid::from_u128(value))
    }

    pub fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }

    pub const fn nil() -> Self {
        Self(uuid::Uuid::nil())
    }

    pub fn is_nil(self) -> bool {
        self == Self::nil()
    }

    pub fn random() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub fn as_non_nil(self) -> Option<Self> {
        if self.is_nil() {
            None
        } else {
            Some(self)
        }
    }

    pub fn verify_non_nil(self) -> Result<(), NilIdError> {
        if self.is_nil() {
            Err(NilIdError::new())
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub struct NilIdError {
    message: Option<String>,
    backtrace: std::backtrace::Backtrace,
}

impl NilIdError {
    #[track_caller]
    pub fn new() -> Self {
        Self {
            message: None,
            backtrace: std::backtrace::Backtrace::capture(),
        }
    }

    #[track_caller]
    pub fn with_message(msg: impl Into<String>) -> Self {
        Self {
            message: Some(msg.into()),
            backtrace: std::backtrace::Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for NilIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Id is nil")?;
        if let Some(msg) = &self.message {
            write!(f, ": {}", msg)?;
        }
        Ok(())
    }
}

impl std::error::Error for NilIdError {
    #[cfg(feature = "unstable")]
    fn provide<'a>(&'a self, req: &mut std::any::Demand<'a>) {
        req.provide_ref(&self.backtrace);
    }
}

pub type CowStr = Cow<'static, str>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[serde(untagged)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
pub enum IdOrIdent {
    Id(Id),
    Name(CowStr),
}

impl std::fmt::Display for IdOrIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Id(id) => write!(f, "{}", id),
            Self::Name(name) => write!(f, "{}", name),
        }
    }
}

#[cfg(feature = "typescript-schema")]
impl ts_rs::TS for IdOrIdent {
    fn name() -> String {
        "IdOrIdent".to_string()
    }

    fn name_with_type_args(args: Vec<String>) -> String {
        assert!(args.is_empty(), "called name_with_type_args on primitive");
        "string".to_string()
    }

    fn decl() -> String {
        "type IdOrIdent = Id | string;".to_string()
    }

    fn inline() -> String {
        "string".to_string()
    }
    fn dependencies() -> Vec<ts_rs::Dependency> {
        vec![]
    }
    fn transparent() -> bool {
        false
    }
}

impl IdOrIdent {
    pub const fn new_static(value: &'static str) -> Self {
        Self::Name(CowStr::Borrowed(value))
    }

    pub fn new_str(value: &str) -> Self {
        if let Ok(id) = uuid::Uuid::from_str(value) {
            Self::Id(Id(id))
        } else {
            Self::Name(value.to_string().into())
        }
    }

    pub fn new_string(value: String) -> Self {
        if let Ok(id) = uuid::Uuid::from_str(&value) {
            Self::Id(Id(id))
        } else {
            Self::Name(value.into())
        }
    }

    pub fn split(self) -> (Option<Id>, Option<CowStr>) {
        match self {
            Self::Id(v) => (Some(v), None),
            Self::Name(v) => (None, Some(v)),
        }
    }

    pub fn as_name(&self) -> Option<&str> {
        if let Self::Name(v) = self {
            Some(v.as_ref())
        } else {
            None
        }
    }

    pub fn as_id(&self) -> Option<Id> {
        match self {
            IdOrIdent::Id(id) => Some(*id),
            IdOrIdent::Name(_) => None,
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

impl From<Id> for IdOrIdent {
    fn from(id: Id) -> Self {
        Self::Id(id)
    }
}

impl From<String> for IdOrIdent {
    fn from(v: String) -> Self {
        Self::new_string(v)
    }
}

impl<'a> From<&'a str> for IdOrIdent {
    fn from(v: &'a str) -> Self {
        Self::new_str(v)
    }
}
