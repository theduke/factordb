use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, bail, Context};
use fnv::FnvHashMap;

use crate::{
    backend::{DbOp, TupleCreate, TupleDelete, TupleOp, TuplePatch, TupleReplace},
    data::{DataMap, Id, IdMap, Ident, Value},
    query,
    schema::{
        self,
        builtin::{self, AttrId},
        AttributeDescriptor, EntityDescriptor,
    },
    AnyError,
};

#[derive(Clone, Debug)]
pub struct Registry {
    entities: FnvHashMap<Id, schema::EntitySchema>,
    entity_idents: HashMap<String, Id>,
    attrs: FnvHashMap<Id, schema::AttributeSchema>,
    attr_idents: HashMap<String, Id>,
}

impl Registry {
    pub fn new() -> Self {
        let mut s = Self {
            entities: Default::default(),
            entity_idents: Default::default(),
            attrs: Default::default(),
            attr_idents: Default::default(),
        };
        s.add_builtins();
        s
    }

    pub fn into_shared(self) -> SharedRegistry {
        Arc::new(RwLock::new(self))
    }

    pub fn duplicate(&self) -> Self {
        self.clone()
    }

    fn add_builtins(&mut self) {
        self.register_attr(builtin::AttrType::schema()).unwrap();

        self.register_attr(builtin::AttrId::schema()).unwrap();
        self.register_attr(builtin::AttrIdent::schema()).unwrap();
        self.register_attr(builtin::AttrValueType::schema())
            .unwrap();
        self.register_attr(builtin::AttrUnique::schema()).unwrap();
        self.register_attr(builtin::AttrIndex::schema()).unwrap();
        self.register_attr(builtin::AttrDescription::schema())
            .unwrap();
        self.register_attr(builtin::AttrStrict::schema()).unwrap();
        self.register_entity(builtin::AttributeSchemaType::schema(), true)
            .unwrap();

        self.register_attr(builtin::AttrAttributes::schema())
            .unwrap();
        self.register_attr(builtin::AttrExtend::schema()).unwrap();
        self.register_attr(builtin::AttrIsRelation::schema())
            .unwrap();
        self.register_entity(builtin::EntitySchemaType::schema(), true)
            .unwrap();
    }

    pub fn attr_by_name(&self, name: &str) -> Option<&schema::AttributeSchema> {
        self.attr_idents.get(name).and_then(|id| self.attrs.get(id))
    }

    pub fn attr_by_ident(&self, ident: &Ident) -> Option<&schema::AttributeSchema> {
        match ident {
            Ident::Id(id) => self.attrs.get(id),
            Ident::Name(name) => self.attr_by_name(name.as_ref()),
        }
    }

    pub fn require_attr_by_name(&self, name: &str) -> Result<&schema::AttributeSchema, AnyError> {
        self.attr_by_name(name)
            .ok_or_else(|| anyhow!("Attr type not found: '{:?}'", name))
    }

    pub fn require_attr_by_ident(
        &self,
        ident: &Ident,
    ) -> Result<&schema::AttributeSchema, AnyError> {
        self.attr_by_ident(ident)
            .ok_or_else(|| anyhow!("Attr type not found: '{:?}'", ident))
    }

    #[inline]
    pub fn require_attr(&self, id: Id) -> Result<&schema::AttributeSchema, AnyError> {
        self.attrs
            .get(&id)
            .ok_or_else(|| anyhow!("Attribute not found: '{}'", id))
    }

    pub fn id_to_data_map(&self, map: IdMap) -> Result<DataMap, AnyError> {
        let ident_map = map
            .into_iter()
            .map(|(id, value)| -> Result<_, AnyError> {
                let ident = self.require_attr(id)?.name.to_string();
                Ok((ident, value))
            })
            .collect::<Result<_, _>>()?;
        Ok(ident_map)
    }

    pub fn data_to_id_map(&self, map: DataMap) -> Result<IdMap, AnyError> {
        let data_map = map
            .into_inner()
            .into_iter()
            .map(|(name, value)| -> Result<_, AnyError> {
                let attr = self.require_attr_by_name(&name)?;
                Ok((attr.id, value))
            })
            .collect::<Result<_, _>>()?;
        Ok(data_map)
    }

    pub fn register_attr(&mut self, attr: schema::AttributeSchema) -> Result<(), AnyError> {
        attr.id
            .verify_non_nil()
            .context("Attribute can not have a nil Id")?;

        if let Some(_old) = self.attrs.get(&attr.id) {
            return Err(anyhow!("Attribute id already exists: '{}'", attr.id));
        }
        if let Some(_old) = self.attr_idents.get(&attr.name) {
            return Err(anyhow!("Attribute ident already exists: '{}'", attr.name));
        }

        self.validate_attr_schema(&attr)?;

        self.attr_idents.insert(attr.name.clone(), attr.id);
        self.attrs.insert(attr.id, attr);
        Ok(())
    }

    pub fn validate_remove_attr(
        &mut self,
        id: Id,
    ) -> Result<crate::schema::AttributeSchema, AnyError> {
        let attr = self
            .attrs
            .remove(&id)
            .ok_or_else(|| anyhow!("Attribute not found: {}", id))?;

        self.entities.values_mut().for_each(|entity| {
            entity.attributes.retain(|ident| match ident {
                Ident::Id(ent_id) => *ent_id != id,
                Ident::Name(name) => name.as_ref() != attr.name,
            });
        });

        Ok(attr)
    }

    pub fn entity_by_name(&self, name: &str) -> Option<&schema::EntitySchema> {
        self.entity_idents
            .get(name)
            .and_then(|id| self.entities.get(id))
    }

    pub fn entity_by_ident(&self, ident: &Ident) -> Option<&schema::EntitySchema> {
        match ident {
            Ident::Id(id) => self.entities.get(id),
            Ident::Name(name) => self.entity_by_name(name.as_ref()),
        }
    }

    pub fn require_entity_by_ident(
        &self,
        ident: &Ident,
    ) -> Result<&schema::EntitySchema, AnyError> {
        self.entity_by_ident(ident)
            .ok_or_else(|| anyhow!("Entity type not found: '{:?}'", ident))
    }

    pub fn register_entity(
        &mut self,
        entity: schema::EntitySchema,
        validate: bool,
    ) -> Result<(), AnyError> {
        entity
            .id
            .verify_non_nil()
            .context("Entity can not have a nil id")?;

        if let Some(_old) = self.entities.get(&entity.id) {
            return Err(anyhow!("Entity id already exists: '{}'", entity.id));
        }
        if validate {
            self.validate_entity_schema(&entity)?;
        }

        self.entity_idents.insert(entity.name.clone(), entity.id);
        self.entities.insert(entity.id, entity);
        Ok(())
    }

    fn validate_attr_schema(&self, attr: &schema::AttributeSchema) -> Result<(), AnyError> {
        if self.attr_idents.get(&attr.name).is_some() {
            bail!("Attribute with name '{}' already exists", attr.name);
        }
        Ok(())
    }

    fn validate_entity_schema(&self, entity: &schema::EntitySchema) -> Result<(), AnyError> {
        if self.entity_idents.get(&entity.name).is_some() {
            bail!("Entity with name '{}' already exists", entity.name);
        }
        if self.entities.contains_key(&entity.id) {
            bail!(
                "Duplicate entity id: '{}' for new entity '{}'",
                entity.id,
                entity.name
            );
        }

        if let Some(extend_ident) = &entity.extend {
            let parent = self.require_entity_by_ident(extend_ident)?;
            if parent.id == entity.id {
                return Err(anyhow!("Entity can't extend itself"));
            }
        }

        // Set for ensuring uniqueness.
        let mut attr_set = HashSet::new();

        for attr_ident in &entity.attributes {
            let attr = self.require_attr_by_ident(attr_ident)?;

            if attr_set.contains(&attr.id) {
                bail!("Duplicate attribute: '{}'", attr.name);
            }
            attr_set.insert(attr.id);
        }

        // FIXME: validate other stuff, like Relation.
        if entity.is_relation {
            bail!("Relations are not implemented yet");
        }

        Ok(())
    }

    fn validate_attr_value(
        &self,
        attr: &schema::AttributeSchema,
        value: Value,
    ) -> Result<Value, AnyError> {
        let ty = value.value_type();
        if ty != attr.value_type {
            return Err(anyhow!(
                "Invalid attribute '{}' - expected a {:?} but got '{:?}'",
                attr.name,
                attr.value_type,
                ty
            ));
        }
        Ok(value)
    }

    // fn make_id_map(
    //     &self,
    //     map: IdentifiableMap,
    //     validate: bool,
    // ) -> Result<FnvHashMap<Id, Value>, AnyError> {
    //     map.into_iter()
    //         .map(|(key, mut value)| {
    //             let attr = self.must_resolve_attr(&key)?;
    //             if validate {
    //                 value = self.validate_attr_value(attr, value)?;
    //             }
    //             Ok((attr.id, value))
    //         })
    //         .collect()
    // }

    fn validate_entity_type_data(
        &self,
        _data: &DataMap,
        _entity: &schema::EntitySchema,
    ) -> Result<(), AnyError> {
        // FIXME: implement!
        todo!()
    }

    fn validate_attributes(&self, mut data: DataMap) -> Result<DataMap, AnyError> {
        for (key, value_ref) in &mut data.0 {
            let attr = self.require_attr_by_name(&key)?;

            let value = std::mem::replace(value_ref, Value::Unit);
            *value_ref = self.validate_attr_value(attr, value)?;
        }

        if let Some(ty) = data_map_get_type(&data)? {
            let entity = self.require_entity_by_ident(&ty)?;
            self.validate_entity_type_data(&data, entity)?;
        }

        Ok(data)
    }

    pub fn validate_create(&self, create: query::update::Create) -> Result<Vec<DbOp>, AnyError> {
        let id = create.id.into_non_nil();

        let mut data = self.validate_attributes(create.data)?;
        data.insert(AttrId::NAME.into(), id.into());

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
            id: create.id,
            data,
        })));

        Ok(ops)
    }

    pub fn validate_replace(
        &self,
        create: query::update::Replace,
        _old: Option<DataMap>,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = create.id.into_non_nil();

        let mut data = self.validate_attributes(create.data)?;
        data.insert(AttrId::NAME.into(), id.into());

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Replace(TupleReplace {
            id: create.id,
            data,
        })));

        // FIXME: cleanup for old data (index removal etc)

        Ok(ops)
    }

    pub fn validate_patch(
        &self,
        patch: query::update::Patch,
        mut old: DataMap,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = patch.id;

        // FIXME: can't use extend here, have to respect list patching etc.
        old.0.extend(patch.data.0.into_iter());
        let data = self.validate_attributes(old)?;

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Patch(TuplePatch { id, data })));

        // FIXME: index updates etc

        Ok(ops)
    }

    pub fn validate_delete(
        &self,
        del: query::update::Delete,
        _old: DataMap,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = del.id;

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Delete(TupleDelete { id })));

        // FIXME: index updates etc

        Ok(ops)
    }
}

fn data_map_get_attr<A: schema::AttributeDescriptor>(map: &DataMap) -> Option<&Value> {
    // TODO: cow for IdOrIdent constructor
    map.get(A::NAME).or_else(|| map.get(A::NAME))
}

fn data_map_get_type(map: &DataMap) -> Result<Option<Ident>, AnyError> {
    match data_map_get_attr::<builtin::AttrType>(map) {
        Some(v) => v
            .to_ident()
            .ok_or_else(|| anyhow!("fabric/type attribute has invalid value"))
            .map(Some),
        None => Ok(None),
    }
}

// fn map_must_get_type(map: &DataMap) -> Result<Ident, AnyError> {
//     let ident = data_map_get_attr::<builtin::AttrType>(map)
//         .ok_or_else(|| anyhow!("fabric/type attribute not present"))?
//         .to_ident()
//         .ok_or_else(|| anyhow!("fabric/type attribute has invalid value"))?;
//     Ok(ident)
// }

pub type SharedRegistry = Arc<RwLock<Registry>>;
