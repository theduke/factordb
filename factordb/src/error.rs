use crate::data::Ident;

// AttributeNotFound

#[derive(Debug)]
pub struct AttributeNotFound {
    ident: Ident,
}

impl AttributeNotFound {
    pub fn new(ident: Ident) -> Self {
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
    ident: Ident,
}

impl IndexNotFound {
    pub fn new(ident: Ident) -> Self {
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
    ident: Ident,
}

impl EntityNotFound {
    pub fn new(ident: Ident) -> Self {
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
    pub entity_id: crate::Id,
    pub attribute: String,
}

impl std::fmt::Display for UniqueConstraintViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Entity '{}' contains duplicate value for unique index '{}' in attribute '{}'",
            self.entity_id, self.index, self.attribute
        )
    }
}

impl std::error::Error for UniqueConstraintViolation {}
