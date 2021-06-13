use std::borrow::Cow;

use crate::AnyError;

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub struct Id(uuid::Uuid);

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Id {
    pub const fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }

    /// Either returns the id if it is not nil, or otherwise creates a new
    /// random one.
    pub fn into_non_nil(self) -> Self {
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

    pub fn verify_non_nil(self) -> Result<(), AnyError> {
        if self.is_nil() {
            Err(anyhow::anyhow!("Id is nil"))
        } else {
            Ok(())
        }
    }
}

pub type CowStr = Cow<'static, str>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum Ident {
    Id(Id),
    Name(CowStr),
}

impl Ident {
    pub const fn new_static(value: &'static str) -> Self {
        Self::Name(CowStr::Borrowed(value))
    }

    pub fn split(self) -> (Option<Id>, Option<CowStr>) {
        match self {
            Self::Id(v) => (Some(v), None),
            Self::Name(v) => (None, Some(v)),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Ident::Id(id) => id.to_string(),
            Ident::Name(n) => n.to_string(),
        }
    }
}

impl From<Id> for Ident {
    fn from(id: Id) -> Self {
        Self::Id(id)
    }
}

impl From<String> for Ident {
    fn from(v: String) -> Self {
        Self::Name(CowStr::from(v))
    }
}

impl<'a> From<&'a str> for Ident {
    fn from(v: &'a str) -> Self {
        Self::Name(Cow::from(v.to_string()))
    }
}
