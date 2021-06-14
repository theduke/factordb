use crate::schema;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeCreate {
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
pub struct EntityDelete {
    pub name: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SchemaAction {
    AttributeCreate(AttributeCreate),
    AttributeDelete(AttributeDelete),
    EntityCreate(EntityCreate),
    EntityDelete(EntityDelete),
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

    pub fn entity_delete(mut self, name: impl Into<String>) -> Self {
        self.actions.push(SchemaAction::EntityDelete(EntityDelete {
            name: name.into(),
        }));
        self
    }
}
