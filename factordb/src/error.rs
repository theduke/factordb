use crate::data::IdOrIdent;

// AttributeNotFound

#[derive(Debug)]
pub struct AttributeNotFound {
    ident: IdOrIdent,
}

impl AttributeNotFound {
    pub fn new(ident: IdOrIdent) -> Self {
        Self { ident }
    }
}

impl std::fmt::Display for AttributeNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Attribute not found: {}", self.ident.to_string())
    }
}

impl std::error::Error for AttributeNotFound {}

// IndexNotFound

#[derive(Debug)]
pub struct IndexNotFound {
    ident: IdOrIdent,
}

impl IndexNotFound {
    pub fn new(ident: IdOrIdent) -> Self {
        Self { ident }
    }
}

impl std::fmt::Display for IndexNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Index not found: {}", self.ident.to_string())
    }
}

impl std::error::Error for IndexNotFound {}

// EntityNotFound

#[derive(Debug)]
pub struct EntityNotFound {
    ident: IdOrIdent,
}

impl EntityNotFound {
    pub fn new(ident: IdOrIdent) -> Self {
        Self { ident }
    }
}

impl std::fmt::Display for EntityNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity not found: {}", self.ident.to_string())
    }
}

impl std::error::Error for EntityNotFound {}

// UnqiueConstraintViolation

#[derive(Debug)]
pub struct UniqueConstraintViolation {
    pub index: String,
    pub entity_id: crate::data::Id,
    pub attribute: String,
    pub value: Option<crate::data::Value>,
}

impl std::fmt::Display for UniqueConstraintViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = self
            .value
            .as_ref()
            .map(|v| format!(": {:?}", v))
            .unwrap_or_default();
        write!(
           f,
            "Unique constraint violation in index '{}': Entity '{}' has duplicate value in attribute '{}'{}",
             self.index, self.entity_id, self.attribute, value
        )
    }
}

impl std::error::Error for UniqueConstraintViolation {}
