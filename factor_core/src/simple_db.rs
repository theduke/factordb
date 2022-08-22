use std::collections::HashMap;

use crate::{
    data::{DataMap, Id, IdOrIdent},
    schema::{
        builtin::{AttrId, AttrIdent},
        AttrMapExt, AttributeMeta, PreCommit,
    },
};

#[derive(Clone, Debug, Default)]
pub struct SimpleDb {
    pub entities: HashMap<Id, DataMap>,
}

impl SimpleDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entity_by_ident(&self, ident: &str) -> Option<&DataMap> {
        self.entities.values().find(|e| {
            if let Some(x) = e.get(AttrIdent::QUALIFIED_NAME).and_then(|x| x.as_str()) {
                ident == x
            } else {
                false
            }
        })
    }

    pub fn entity_by_ident_mut(&mut self, ident: &str) -> Option<&mut DataMap> {
        self.entities.values_mut().find(|e| {
            if let Some(x) = e.get(AttrIdent::QUALIFIED_NAME).and_then(|x| x.as_str()) {
                ident == x
            } else {
                false
            }
        })
    }

    pub fn entities_by_type(&self, class: &str) -> impl Iterator<Item = &DataMap> {
        let (class_id, class_ident) = match IdOrIdent::new_str(class) {
            IdOrIdent::Id(id) => {
                let ident = self
                    .entities
                    .get(&id)
                    .and_then(|e| e.get(AttrIdent::QUALIFIED_NAME).and_then(|x| x.as_str()))
                    .map(|x| x.to_string());
                (Some(id), ident)
            }
            IdOrIdent::Name(name) => {
                let id = self.entity_by_ident(&name).map(|e| e.get_id().unwrap());
                (id, Some(name.to_string()))
            }
        };

        self.entities.values().filter(move |e| {
            if let Some(ty) = e.get_type() {
                match ty {
                    IdOrIdent::Id(id) => Some(id) == class_id,
                    IdOrIdent::Name(name) => Some(name.as_ref()) == class_ident.as_deref(),
                }
            } else {
                false
            }
        })
    }

    pub fn apply_pre_commit(mut self, commit: PreCommit) -> Result<Self, anyhow::Error> {
        let subject = IdOrIdent::new_str(&commit.subject);

        let old_opt = match &subject {
            IdOrIdent::Id(id) => self.entities.get_mut(id),
            IdOrIdent::Name(name) => self.entity_by_ident_mut(name),
        };

        if let Some(old) = old_opt {
            let id = old.get_id().unwrap();

            if commit.destroy {
                self.entities.remove(&id);
            } else if let Some(mut set) = commit.set {
                if commit.replace {
                    set.insert(AttrId::QUALIFIED_NAME.to_string(), id.into());
                    *old = set;
                } else {
                    for (key, value) in set.into_iter() {
                        old.insert(key.clone(), value.clone());
                    }
                }
            }
        } else {
            let mut data = commit.set.unwrap_or_default();
            let id = if let Some(id) = data.get_id() {
                id
            } else {
                let id = Id::random();
                data.insert(AttrId::QUALIFIED_NAME.to_string(), id.into());
                id
            };
            if let IdOrIdent::Name(ident) = &subject {
                data.insert(
                    AttrIdent::QUALIFIED_NAME.to_string(),
                    ident.to_string().into(),
                );
            }
            self.entities.insert(id, data);
        }

        Ok(self)
    }
}
