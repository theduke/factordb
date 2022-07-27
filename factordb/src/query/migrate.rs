use anyhow::{anyhow, bail};
use schema::{AttributeSchema, EntitySchema};

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

impl Default for Migration {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a list of migrations into a single migration that re-creates
/// the final state.
pub fn unify_migrations(migrations: Vec<Migration>) -> Result<Migration, anyhow::Error> {
    let mut attributes = Vec::<AttributeSchema>::new();
    let mut entities = Vec::<EntitySchema>::new();
    let mut indexes = Vec::<IndexSchema>::new();

    for mig in migrations {
        for action in mig.actions {
            match action {
                SchemaAction::AttributeCreate(create) => {
                    let attr = attributes.iter().find(|c| c.ident == create.schema.ident);

                    if let Some(old) = attr {
                        if old != &create.schema {
                            bail!("Duplicate AttributeCreate action for attr {} - merging multiple creates is not supported yet", create.schema.ident);
                        }
                    } else {
                        attributes.push(create.schema);
                    }
                }
                SchemaAction::AttributeUpsert(upsert) => {
                    let attr = attributes.iter().find(|c| c.ident == upsert.schema.ident);

                    if let Some(old) = attr {
                        if old != &upsert.schema {
                            bail!(
                                "Unsupported AttributeUpsert action for attr {} - merging upsert with previous create is not supported yet", 
                                  upsert.schema.ident
                            );
                        }
                    } else {
                        attributes.push(upsert.schema);
                    }
                }
                SchemaAction::AttributeChangeType(change) => {
                    let attr = attributes
                        .iter_mut()
                        .find(|c| c.ident == change.attribute)
                        .ok_or_else(|| anyhow!("Invalid AttributeChangeType action for attr {}: attribute not created yet", change.attribute))?;

                    attr.value_type = change.new_type;
                }
                SchemaAction::AttributeCreateIndex(cindex) => {
                    let attr = attributes
                        .iter_mut().find(|a| a.ident == cindex.attribute)
                        .ok_or_else(|| anyhow!("Invalid AttributeChangeType action for attr {}: attribute not created yet", cindex.attribute))?;

                    if cindex.unique {
                        attr.unique = true;
                    } else {
                        attr.index = true;
                    }
                }
                SchemaAction::AttributeDelete(del) => {
                    attributes.retain(|a| a.ident != del.name);
                }
                SchemaAction::EntityCreate(create) => {
                    let entity = entities.iter().find(|e| e.ident == create.schema.ident);

                    if let Some(old) = entity {
                        if old != &create.schema {
                            bail!("Duplicate EntityCreate action for attr {} - merging multiple creates is not supported yet", create.schema.ident);
                        }
                    } else {
                        entities.push(create.schema);
                    }
                }
                SchemaAction::EntityAttributeAdd(add) => {
                    let entity = entities.iter_mut().find(|e| e.ident == add.entity)
                    .ok_or_else(|| anyhow!("Invalid EntityAttributeAdd action for attr {}: entity not created yet", add.attribute))?;

                    let old_attr = entity
                        .attributes
                        .iter_mut()
                        .find(|a| a.attribute == add.attribute.clone().into());

                    if let Some(old) = old_attr {
                        old.cardinality = add.cardinality;
                    } else {
                        entity.attributes.push(schema::EntityAttribute {
                            attribute: add.attribute.clone().into(),
                            cardinality: add.cardinality,
                        });
                    }
                }
                SchemaAction::EntityAttributeChangeCardinality(change) => {
                    let entity = entities.iter_mut().find(|e| e.ident == change.entity_type)
                    .ok_or_else(|| anyhow!("Invalid EntityAttributeAdd action for attr {}: entity not created yet", change.attribute))?;

                    let old_attr = entity
                        .attributes
                        .iter_mut()
                        .find(|a| a.attribute == change.attribute.clone().into())
                        .ok_or_else(|| anyhow!("Invalid EntityAttributeChangeCardinality action for attr {}: attribute not added yet", change.attribute))?;

                    old_attr.cardinality = change.new_cardinality;
                }
                SchemaAction::EntityAttributeRemove(remove) => {
                    let entity = entities.iter_mut().find(|e| e.ident == remove.entity_type)
                    .ok_or_else(|| anyhow!("Invalid EntityAttributeRemove action for attr {}: entity not created yet", remove.attribute))?;

                    entity
                        .attributes
                        .retain(|a| a.attribute != remove.attribute.clone().into());
                }
                SchemaAction::EntityUpsert(upsert) => {
                    let entity = entities.iter().find(|e| e.ident == upsert.schema.ident);

                    if let Some(old) = entity {
                        if old != &upsert.schema {
                            bail!(
                                "Unsupported EntityUpsert action for entity {} - merging upsert with previous create is not supported yet", 
                                  upsert.schema.ident
                            );
                        }
                    } else {
                        entities.push(upsert.schema);
                    }
                }
                SchemaAction::EntityDelete(del) => {
                    entities.retain(|e| e.ident != del.name);
                }
                SchemaAction::IndexCreate(create) => {
                    let old_create = indexes.iter().find(|i| i.ident == create.schema.ident);

                    if let Some(old) = old_create {
                        if old != &create.schema {
                            bail!("Duplicate IndexCreate action for attr {} - merging multiple creates is not supported yet", create.schema.ident);
                        }
                    } else {
                        indexes.push(create.schema);
                    }
                }
                SchemaAction::IndexDelete(del) => {
                    indexes.retain(|i| i.ident != del.name);
                }
            }
        }
    }

    let attr_create = attributes
        .into_iter()
        .map(|a| SchemaAction::from(AttributeCreate { schema: a }));
    let entity_creates = entities
        .into_iter()
        .map(|e| SchemaAction::from(EntityCreate { schema: e }));
    let index_creates = indexes
        .into_iter()
        .map(|i| SchemaAction::from(IndexCreate { schema: i }));

    let main = Migration {
        name: None,
        actions: attr_create
            .chain(entity_creates)
            .chain(index_creates)
            .collect(),
    };

    Ok(main)
}
