use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, bail, Context};
use fnv::FnvHashMap;
use schema::Cardinality;

use crate::{
    backend::{DbOp, TupleCreate, TupleDelete, TupleMerge, TupleOp, TupleReplace},
    data::{DataMap, Id, IdMap, Ident, Value, ValueType},
    query,
    schema::{
        self,
        builtin::{self, AttrId},
        AttributeDescriptor, EntityDescriptor,
    },
    AnyError,
};

const MAX_NAME_LEN: usize = 50;

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

    /// Reset all state.
    /// Removes all registered entities and attributes, but restores the
    /// builtins.
    pub fn reset(&mut self) {
        self.entities.clear();
        self.entity_idents.clear();
        self.attrs.clear();
        self.attr_idents.clear();

        self.add_builtins();
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
            entity.attributes.retain(|field| match &field.attribute {
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

        match &attr.value_type {
            x if x.is_scalar() => {}
            ValueType::Object(obj) => {
                for field in &obj.fields {
                    if field.name.len() > MAX_NAME_LEN {
                        return Err(anyhow!(
                            "object field '{}' exceeds maximum field name length of {}",
                            field.name,
                            MAX_NAME_LEN
                        ));
                    }
                }
            }
            other => {
                return Err(anyhow!(
                    "Invalid attribute type {:?} - attributes must be scalar values",
                    other,
                ));
            }
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

        for field in &entity.attributes {
            let attr = self.require_attr_by_ident(&field.attribute)?;

            if attr_set.contains(&attr.id) {
                bail!("Duplicate attribute: '{}'", attr.name);
            }
            attr_set.insert(attr.id);
        }

        // FIXME: validate other stuff, like Relation.

        Ok(())
    }

    /// Valdiate that a value conforms to a value type.
    /// Coerces values into the desired type where appropriate.
    fn validate_coerce_value_named(
        name: &str,
        ty: &ValueType,
        value: &mut Value,
    ) -> Result<(), AnyError> {
        match ty {
            ValueType::Unit
            | ValueType::Bool
            | ValueType::Int
            | ValueType::UInt
            | ValueType::Float
            | ValueType::String
            | ValueType::Map
            | ValueType::Bytes => {
                let actual_ty = value.value_type();
                if &actual_ty != ty {
                    return Err(anyhow!(
                        "Invalid attribute '{}' - expected a {:?} but got '{:?}'",
                        name,
                        ty,
                        actual_ty
                    ));
                }
            }
            ValueType::List(_item_type) => {
                panic!("Internal error: List is not a valid ValueType for attributes");
            }
            ValueType::Any => {
                // Everything is allowed.
            }
            ValueType::Union(variants) => {
                for variant_ty in variants {
                    if Self::validate_coerce_value_named(name, variant_ty, value).is_ok() {
                        return Ok(());
                    }
                }
                return Err(anyhow!(
                    "Invalid attribute '{}' - does not conform to any variant of '{:?}'",
                    name,
                    variants,
                ));
            }
            ValueType::Object(_obj) => {
                // FIXME: validate objects properly.

                let actual_ty = value.value_type();
                if &actual_ty != ty {
                    return Err(anyhow!(
                        "Invalid attribute '{}' - expected a {:?} but got '{:?}'",
                        name,
                        ty,
                        actual_ty
                    ));
                }
            }
            ValueType::DateTime => {
                if !value.is_uint() {
                    // TODO: coerce?
                    return Err(anyhow!("Invalid timestamp - must be an unsigned integer"));
                }
            }
            ValueType::Url => {
                if let Some(v) = value.as_str() {
                    if let Err(_err) = url::Url::parse(v) {
                        return Err(anyhow!(
                            "Invalid attribute '{}' - expected a valid URL",
                            name
                        ));
                    }
                } else {
                    return Err(anyhow!("Invalid url - expected an integer"));
                }
            }
            ValueType::Ref => {
                match value {
                    Value::String(strval) => {
                        // TODO: resolve idents?
                        if let Err(_err) = uuid::Uuid::from_str(strval) {
                            return Err(anyhow!(
                                "Invalid attribute '{}' - expected a valid id (UUID)",
                                name
                            ));
                        }
                    }
                    Value::Id(_) => {
                        // Ok
                    }
                    _other => {
                        return Err(anyhow!(
                            "Invalid attribute '{}' - expected a valid ID (UUID)",
                            name
                        ))
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_attr_value(
        &self,
        attr: &schema::AttributeSchema,
        value: &mut Value,
    ) -> Result<(), AnyError> {
        Self::validate_coerce_value_named(&attr.name, &attr.value_type, value)
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

    fn validate_entity_data(
        &self,
        data: &mut DataMap,
        entity: &schema::EntitySchema,
    ) -> Result<(), AnyError> {
        for field in &entity.attributes {
            // TODO: create a static list of fields for each entity so that
            // we don't have to do this lookup each time.
            let attr = self.require_attr_by_ident(&field.attribute)?;

            match (data.get_mut(&attr.name), field.cardinality) {
                (None, Cardinality::Optional) => {}
                (None, Cardinality::Required) => {
                    return Err(anyhow!("Missing required attribute '{}'", attr.name));
                }
                (None, Cardinality::Many) => {
                    // We could insert a list here, but that decision is
                    // probably better left to the backend.
                }
                (Some(value), Cardinality::Optional) => {
                    self.validate_attr_value(attr, value)?;
                }
                (Some(value), Cardinality::Required) => {
                    self.validate_attr_value(attr, value)?;
                }
                (Some(value), Cardinality::Many) => match value {
                    Value::List(items) => {
                        for item in items {
                            self.validate_attr_value(attr, item)?;
                        }
                    }
                    single => {
                        self.validate_attr_value(attr, single)?;
                        let value = std::mem::replace(single, Value::Unit);
                        *single = Value::List(vec![value]);
                    }
                },
            }
        }

        // FIXME: if entity is strict, validate that no extra fields are present

        Ok(())
    }

    fn validate_attributes(&self, mut data: DataMap) -> Result<DataMap, AnyError> {
        if let Some(ty) = data_map_get_type(&data)? {
            let entity = self.require_entity_by_ident(&ty)?;
            self.validate_entity_data(&mut data, entity)?;
        } else {
            for (key, value) in &mut data.0 {
                let attr = self.require_attr_by_name(&key)?;
                self.validate_attr_value(attr, value)?;
            }
        }

        Ok(data)
    }

    pub fn validate_create(&self, create: query::mutate::Create) -> Result<Vec<DbOp>, AnyError> {
        let id = create.id.non_nil_or_randomize();

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
        create: query::mutate::Replace,
        _old: Option<DataMap>,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = create.id.non_nil_or_randomize();

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

    pub fn validate_merge(
        &self,
        merge: query::mutate::Merge,
        mut old: DataMap,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = merge.id.non_nil_or_randomize();

        // FIXME: can't use extend here, have to respect list patching etc.
        old.0.extend(merge.data.0.into_iter());
        let mut data = self.validate_attributes(old)?;
        data.insert(AttrId::NAME.into(), id.into());

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Merge(TupleMerge { id, data })));

        // FIXME: index updates etc

        Ok(ops)
    }

    pub fn validate_delete(
        &self,
        del: query::mutate::Delete,
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
