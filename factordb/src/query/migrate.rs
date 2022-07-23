use crate::{
    data::Value,
    prelude::ValueType,
    schema::{self, Cardinality, IndexSchema},
};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeCreate {
    pub schema: schema::AttributeSchema,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntityAttributeAdd {
    /// The qualified name of the entity.
    pub entity: String,
    /// The qualified name of the attribute to add.
    pub attribute: String,
    /// Cardinality for the attribute.
    pub cardinality: Cardinality,
    /// Optional default value.
    /// This is required if the cardinality is [`Cardinality::Required`].
    pub default_value: Option<Value>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntityAttributeChangeCardinality {
    pub entity_type: String,
    pub attribute: String,
    pub new_cardinality: Cardinality,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntityAttributeRemove {
    pub entity_type: String,
    pub attribute: String,
    #[serde(default)]
    pub delete_values: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeUpsert {
    pub schema: schema::AttributeSchema,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeChangeType {
    pub attribute: String,
    pub new_type: ValueType,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeCreateIndex {
    pub attribute: String,
    pub unique: bool,
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
    pub delete_all: bool,
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
    AttributeChangeType(AttributeChangeType),
    AttributeCreateIndex(AttributeCreateIndex),
    AttributeDelete(AttributeDelete),
    EntityCreate(EntityCreate),
    EntityAttributeAdd(EntityAttributeAdd),
    EntityAttributeChangeCardinality(EntityAttributeChangeCardinality),
    EntityAttributeRemove(EntityAttributeRemove),
    EntityUpsert(EntityUpsert),
    EntityDelete(EntityDelete),
    IndexCreate(IndexCreate),
    IndexDelete(IndexDelete),
}

impl From<IndexDelete> for SchemaAction {
    fn from(action: IndexDelete) -> Self {
        SchemaAction::IndexDelete(action)
    }
}

impl From<IndexCreate> for SchemaAction {
    fn from(action: IndexCreate) -> Self {
        SchemaAction::IndexCreate(action)
    }
}

impl From<EntityDelete> for SchemaAction {
    fn from(action: EntityDelete) -> Self {
        SchemaAction::EntityDelete(action)
    }
}

impl From<EntityUpsert> for SchemaAction {
    fn from(action: EntityUpsert) -> Self {
        SchemaAction::EntityUpsert(action)
    }
}

impl From<EntityAttributeRemove> for SchemaAction {
    fn from(action: EntityAttributeRemove) -> Self {
        SchemaAction::EntityAttributeRemove(action)
    }
}

impl From<EntityAttributeChangeCardinality> for SchemaAction {
    fn from(action: EntityAttributeChangeCardinality) -> Self {
        SchemaAction::EntityAttributeChangeCardinality(action)
    }
}

impl From<EntityAttributeAdd> for SchemaAction {
    fn from(action: EntityAttributeAdd) -> Self {
        SchemaAction::EntityAttributeAdd(action)
    }
}

impl From<EntityCreate> for SchemaAction {
    fn from(action: EntityCreate) -> Self {
        SchemaAction::EntityCreate(action)
    }
}

impl From<AttributeDelete> for SchemaAction {
    fn from(action: AttributeDelete) -> Self {
        SchemaAction::AttributeDelete(action)
    }
}

impl From<AttributeCreateIndex> for SchemaAction {
    fn from(action: AttributeCreateIndex) -> Self {
        SchemaAction::AttributeCreateIndex(action)
    }
}

impl From<AttributeChangeType> for SchemaAction {
    fn from(action: AttributeChangeType) -> Self {
        SchemaAction::AttributeChangeType(action)
    }
}

impl From<AttributeUpsert> for SchemaAction {
    fn from(action: AttributeUpsert) -> Self {
        SchemaAction::AttributeUpsert(action)
    }
}

impl From<AttributeCreate> for SchemaAction {
    fn from(action: AttributeCreate) -> Self {
        SchemaAction::AttributeCreate(action)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    pub name: Option<String>,
    pub actions: Vec<SchemaAction>,
}

impl Migration {
    pub fn new() -> Self {
        Self {
            name: None,
            actions: Vec::new(),
        }
    }

    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            actions: Vec::new(),
        }
    }

    pub fn action(mut self, action: SchemaAction) -> Self {
        self.actions.push(action);
        self
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

    pub fn attr_change_type(mut self, attribute: impl Into<String>, new_type: ValueType) -> Self {
        self.actions
            .push(SchemaAction::AttributeChangeType(AttributeChangeType {
                attribute: attribute.into(),
                new_type,
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

    pub fn entity_delete(mut self, name: impl Into<String>, delete_all: bool) -> Self {
        self.actions.push(SchemaAction::EntityDelete(EntityDelete {
            name: name.into(),
            delete_all,
        }));
        self
    }
}
