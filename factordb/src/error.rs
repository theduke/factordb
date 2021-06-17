use crate::data::Ident;

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
