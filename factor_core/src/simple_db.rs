use std::collections::HashMap;

use anyhow::{bail, Context};

use crate::{
    data::{DataMap, Id, IdOrIdent},
    schema::{
        builtin::{AttrId, AttrIdent},
        dsl, AttrMapExt, AttributeMeta,
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

    pub fn resolve_commit(
        &self,
        config: &NamespaceConfig,
        commit: dsl::DslCommit,
        skip_resolve_namespaced: bool,
    ) -> Result<dsl::DslCommit, anyhow::Error> {
        fn has_namespace(value: &str) -> bool {
            value.contains('/')
        }

        let ctx = NamespaceContext::from_simple_db(&config, &self)?;

        dbg!(&ctx);

        let mut c = commit;

        let ns = &config.namespace;

        if !has_namespace(&c.subject) {
            c.subject = format!("{ns}/{}", c.subject);
        }

        if let Some(set) = c.set.take() {
            let mut new_set = DataMap::new();

            for (key, value) in set.iter() {
                let full_key = if has_namespace(key) {
                    key.clone()
                } else {
                    ctx.objects
                        .get(key)
                        .with_context(|| format!("Could not resolve property '{key}'"))?
                        .clone()
                };

                if !(has_namespace(key) && skip_resolve_namespaced) {
                    let property = self
                        .entity_by_ident(&full_key)
                        .with_context(|| format!("Could not resolve property '{full_key}'"))?;

                    // Make sure the object is a property.
                    // TODO: use const for factor/Property
                    if !property
                        // TODO: use const
                        .get("factor/type")
                        .and_then(|t| t.as_str())
                        .filter(|c| *c == "factor/Property")
                        .is_some()
                    {
                        dbg!(&property);
                        bail!("Invalid property '{key}': entity is not a property");
                    }

                    // If the property type is Ref, resolve the target.
                    // TODO: use const
                    let ty_raw = property.get("factor/valueType").with_context(|| {
                        format!("invalid property {full_key}: property has no valueType")
                    })?;
                    let ty = ty_raw.as_str().with_context(|| {
                        format!("invalid property {full_key}: factor/valueType must be a string")
                    })?;

                    // TODO: use const
                    // TODO: handle arrays of references and other nested references...
                    let final_value = if ty == "factor/Reference" {
                        let target = value.as_str().with_context(|| {
                        "invalid property value {full_key}: {value:?} - references must be a string"
                    })?;

                        let full_target = if has_namespace(target) {
                            target.to_string()
                        } else {
                            ctx.objects
                                .get(target)
                                .with_context(|| format!("Could not resolve entity {target}"))?
                                .clone()
                        };
                        full_target.into()
                    } else {
                        value.clone()
                    };

                    new_set.insert(full_key, final_value);
                } else {
                    new_set.insert(full_key, value.clone());
                }
            }

            c.set = Some(new_set);
        }

        Ok(c)
    }

    pub fn apply_dsl_commit(mut self, commit: dsl::DslCommit) -> Result<Self, anyhow::Error> {
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
            } else {
                data.insert(AttrIdent::QUALIFIED_NAME.to_string(), commit.subject.into());
            }
            self.entities.insert(id, data);
        }

        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub struct NamespaceConfig {
    pub namespace: String,
    pub imports: Vec<NamespaceImport>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct NamespaceImport {
    pub source: String,
    pub alias: Option<String>,
}

#[derive(Debug)]
pub struct NamespaceContext {
    /// Map from flat name to namespaced name.
    pub objects: HashMap<String, String>,
}

impl NamespaceContext {
    fn from_simple_db(config: &NamespaceConfig, db: &SimpleDb) -> Result<Self, anyhow::Error> {
        let mut objects = HashMap::new();

        let mut prefixes: Vec<String> = config
            .imports
            .iter()
            .map(|imp| {
                if imp.source.ends_with('/') {
                    imp.source.clone()
                } else {
                    format!("{}/", imp.source)
                }
            })
            .collect();
        prefixes.push(format!("{}/", config.namespace));

        for entity in db.entities.values() {
            let Some(id_raw) = entity.get(AttrIdent::QUALIFIED_NAME) else {
                continue;
            };
            let id = id_raw
                .as_str()
                .with_context(|| format!("Invalid databse state: factor/ident is not a string"))?;

            let is_match = prefixes.iter().any(|prefix| id.starts_with(prefix));
            if !is_match {
                continue;
            }

            let plain = id.split('/').last().unwrap();

            if let Some(existing) = objects.get(plain) {
                bail!(
                    "name clash for id '{id}':  context already has name {plain} as id {existing}"
                )
            }
            objects.insert(plain.to_string(), id.to_string());
        }

        dbg!(db, config, &objects);

        Ok(Self { objects })
    }
}
