use crate::schema::{self, IndexSchema};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeCreate {
    pub schema: schema::AttributeSchema,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeUpsert {
    pub schema: schema::AttributeSchema,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeDelete {
    pub name: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntityCreate {
    pub schema: schema::EntitySchema,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntityUpsert {
    pub schema: schema::EntitySchema,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntityDelete {
    pub name: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IndexCreate {
    pub schema: IndexSchema,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IndexDelete {
    pub name: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SchemaAction {
    AttributeCreate(AttributeCreate),
    AttributeUpsert(AttributeUpsert),
    AttributeDelete(AttributeDelete),
    EntityCreate(EntityCreate),
    EntityUpsert(EntityUpsert),
    EntityDelete(EntityDelete),
    IndexCreate(IndexCreate),
    IndexDelete(IndexDelete),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    pub actions: Vec<SchemaAction>,
}

impl Migration {
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
        }
    }

    pub fn attr_create(mut self, attr: schema::AttributeSchema) -> Self {
        self.actions
            .push(SchemaAction::AttributeCreate(AttributeCreate {
                schema: attr,
            }));
        self
    }

    pub fn attr_upsert(mut self, attr: schema::AttributeSchema) -> Self {
        self.actions
            .push(SchemaAction::AttributeUpsert(AttributeUpsert {
                schema: attr,
            }));
        self
    }

    pub fn attr_delete(mut self, name: impl Into<String>) -> Self {
        self.actions
            .push(SchemaAction::AttributeDelete(AttributeDelete {
                name: name.into(),
            }));
        self
    }

    pub fn entity_create(mut self, entity: schema::EntitySchema) -> Self {
        self.actions
            .push(SchemaAction::EntityCreate(EntityCreate { schema: entity }));
        self
    }

    pub fn entity_upsert(mut self, entity: schema::EntitySchema) -> Self {
        self.actions
            .push(SchemaAction::EntityUpsert(EntityUpsert { schema: entity }));
        self
    }

    pub fn entity_delete(mut self, name: impl Into<String>) -> Self {
        self.actions.push(SchemaAction::EntityDelete(EntityDelete {
            name: name.into(),
        }));
        self
    }
}
