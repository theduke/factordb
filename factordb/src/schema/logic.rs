use anyhow::anyhow;

use crate::{
    backend::{DbOp, SelectOpt, TupleCreate, TupleMerge, TupleOp},
    data::{
        value::{self, to_value, to_value_map},
        Id,
    },
    query::migrate::{Migration, SchemaAction},
    registry::Registry,
    schema::{builtin, AttrMapExt},
    AnyError, Ident,
};

use super::{builtin::AttrType, AttributeDescriptor, Cardinality, EntityAttribute};

enum EntityAttributePatch {
    Added(EntityAttribute),
    Removed(EntityAttribute),
    CardinalityChanged {
        old: Cardinality,
        new: Cardinality,
        attribute: Ident,
    },
}

fn diff_attributes(old: &[EntityAttribute], new: &[EntityAttribute]) -> Vec<EntityAttributePatch> {
    let mut patches = Vec::new();

    for old_attr in old {
        if let Some(new_attr) = new.iter().find(|attr| attr.attribute == old_attr.attribute) {
            if old_attr.cardinality != new_attr.cardinality {
                patches.push(EntityAttributePatch::CardinalityChanged {
                    old: old_attr.cardinality,
                    new: new_attr.cardinality,
                    attribute: old_attr.attribute.clone(),
                })
            }
        } else {
            patches.push(EntityAttributePatch::Removed(old_attr.clone()));
        }
    }

    for new_attr in new {
        let already_exists = old
            .iter()
            .any(|old_attr| old_attr.attribute == new_attr.attribute);
        if !already_exists {
            patches.push(EntityAttributePatch::Added(new_attr.clone()));
        }
    }

    patches
}

/// Validate a migration against the registry.
///
/// NOTE: is_internal must be set to false for regular migrations, and to true
/// for internal migrations driven by the factor db.
/// With is_internal = false, any changes to builtin entities/attributes are
/// rejected.
pub fn validate_migration(
    reg: &mut Registry,
    mut mig: Migration,
    is_internal: bool,
) -> Result<(Migration, Vec<DbOp>), AnyError> {
    let mut ops = Vec::new();

    for action in &mut mig.actions {
        match action {
            SchemaAction::AttributeCreate(create) => {
                let namespace = create.schema.parse_namespace()?;
                if namespace == super::builtin::NS_FACTOR && !is_internal {
                    return Err(anyhow!("Invalid namespace: factor/ is reserved"));
                }

                create.schema.id = create.schema.id.non_nil_or_randomize();

                reg.register_attribute(create.schema.clone())?;

                let mut data = value::to_value_map(create.schema.clone())?;
                // Add tye factor/type attr.
                data.insert(
                    AttrType::QUALIFIED_NAME.to_string(),
                    super::builtin::ATTRIBUTE_ID.into(),
                );

                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: create.schema.id,
                    data,
                })));
            }
            SchemaAction::AttributeUpsert(upsert) => {
                let namespace = upsert.schema.parse_namespace()?;
                if namespace == super::builtin::NS_FACTOR && !is_internal {
                    return Err(anyhow!("Invalid namespace: factor/ is reserved"));
                }

                let attr = &mut upsert.schema;

                match reg.attr_by_ident(&attr.ident.clone().into()) {
                    Some(old) => {
                        if !attr.id.is_nil() && attr.id != old.schema.id {
                            return Err(anyhow!(
                                "Id mismatch: attribute name already exists with id {}",
                                old.schema.id
                            ));
                        } else {
                            attr.id = old.schema.id;
                        }
                        if attr != &old.schema {
                            return Err(anyhow!("Attribute '{}' has changed - upsert with a changed attribute schema is not supported (yet)\n\nold: {:?}\n\n new: {:?}", attr.ident, old, attr));
                        } else {
                            continue;
                        }
                    }
                    None => {}
                };

                // TODO: re-use create code from above by factoring out to
                // extra function.
                attr.id = attr.id.non_nil_or_randomize();

                reg.register_attribute(upsert.schema.clone())?;

                let mut data = value::to_value_map(upsert.schema.clone())?;
                // Add tye factor/type attr.
                data.insert(
                    AttrType::QUALIFIED_NAME.to_string(),
                    super::builtin::ATTRIBUTE_ID.into(),
                );

                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: upsert.schema.id,
                    data,
                })));
            }

            SchemaAction::AttributeDelete(del) => {
                let attr = reg.require_attr_by_name(&del.name)?.clone();

                if attr.namespace == super::builtin::NS_FACTOR {
                    return Err(anyhow!("Invalid namespace: factor/ is reserved"));
                }

                let op = DbOp::Select(SelectOpt {
                    selector: crate::query::expr::Expr::literal(true),
                    op: TupleOp::RemoveAttrs(crate::backend::TupleRemoveAttrs {
                        id: Id::nil(),
                        attrs: vec![attr.schema.id],
                    }),
                });

                ops.push(op);
            }
            SchemaAction::EntityCreate(create) => {
                let namespace = create.schema.parse_namespace()?;
                if !is_internal && namespace == super::builtin::NS_FACTOR {
                    return Err(anyhow!(
                        "Invalid entity ident: the factor/ namespace is reserved"
                    ));
                }

                create.schema.id = create.schema.id.non_nil_or_randomize();

                reg.register_entity(create.schema.clone(), true)?;

                let mut data = value::to_value_map(create.schema.clone())?;
                // Add tye factor/type attr.
                data.insert(
                    AttrType::QUALIFIED_NAME.to_string(),
                    super::builtin::ENTITY_ID.into(),
                );

                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: create.schema.id,
                    data,
                })));
            }
            SchemaAction::EntityUpsert(upsert) => {
                let namespace = upsert.schema.parse_namespace()?;
                if !is_internal && namespace == super::builtin::NS_FACTOR {
                    return Err(anyhow!(
                        "Invalid entity ident: the factor/ namespace is reserved"
                    ));
                }

                let entity = &mut upsert.schema;
                match reg.entity_by_ident(&entity.ident.clone().into()) {
                    Some(old) => {
                        if !entity.id.is_nil() && entity.id != old.schema.id {
                            return Err(anyhow!(
                                "Id mismatch: entity name already exists with id {}",
                                old.schema.id
                            ));
                        } else {
                            entity.id = old.schema.id;
                        }

                        if entity != &old.schema {
                            if old.schema.ident != entity.ident {
                                return Err(anyhow!(
                                    "Entity upsert with changed ident is not supported"
                                ));
                            }
                            if old.schema.extends != entity.extends {
                                return Err(anyhow!(
                                    "Entity upsert with changed extend parent schemas is not supported"
                                ));
                            }
                            if old.schema.strict != entity.strict {
                                return Err(anyhow!(
                                    "Entity upsert with changed strict setting is not supported"
                                ));
                            }

                            // Entity has changed.
                            // Check what has changed and if we can allow an
                            // upsert.

                            let mut merge = to_value_map(&old.schema)?;

                            if old.schema.title != entity.title {
                                if let Some(new_title) = &entity.title {
                                    merge.insert_attr::<builtin::AttrTitle>(new_title.clone());
                                } else {
                                    merge.remove(builtin::AttrTitle::QUALIFIED_NAME);
                                }
                            }

                            if old.schema.description != entity.description {
                                if let Some(new_description) = &entity.description {
                                    merge.insert_attr::<builtin::AttrDescription>(
                                        new_description.clone(),
                                    );
                                } else {
                                    merge.remove(builtin::AttrDescription::QUALIFIED_NAME);
                                }
                            }

                            if old.schema.attributes != entity.attributes {
                                let diffs =
                                    diff_attributes(&old.schema.attributes, &entity.attributes);

                                let mut new_attrs = old.schema.attributes.clone();

                                for diff in diffs {
                                    match diff {
                                        EntityAttributePatch::Added(new_attr) => {
                                            if new_attr.cardinality != Cardinality::Required {
                                                new_attrs.push(new_attr);
                                            } else {
                                                return Err(anyhow!(
                                                    "Entity upsert with new attribute '{:?}' is invalid - new attributes must have a cardinality of Optional or Many",
                                                    new_attr.attribute,
                                                ));
                                            }
                                        }
                                        EntityAttributePatch::Removed(removed) => {
                                            return Err(anyhow!(
                                                "Entity upsert can not remove attributes (attribute '{:?}')",
                                                removed.attribute,
                                            ));
                                        }
                                        EntityAttributePatch::CardinalityChanged {
                                            old: _,
                                            new: _,
                                            attribute,
                                        } => {
                                            return Err(anyhow!(
                                                "Entity upsert can not change attribute cardinality (attribute '{:?}')",
                                                attribute,
                                            ));
                                        }
                                    }
                                }

                                let new_attrs_value = to_value(new_attrs)?;
                                merge.insert(
                                    builtin::AttrAttributes::QUALIFIED_NAME.into(),
                                    new_attrs_value,
                                );
                            }

                            ops.push(DbOp::Tuple(TupleOp::Merge(TupleMerge {
                                id: entity.id,
                                data: merge,
                            })));
                            continue;
                        } else {
                            continue;
                        }
                    }
                    None => {}
                }

                entity.id = entity.id.non_nil_or_randomize();

                reg.register_entity(entity.clone(), true)?;

                let mut data = value::to_value_map(upsert.schema.clone())?;
                // Add tye factor/type attr.
                data.insert(
                    AttrType::QUALIFIED_NAME.to_string(),
                    super::builtin::ENTITY_ID.into(),
                );

                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: upsert.schema.id,
                    data,
                })));
            }
            SchemaAction::EntityDelete(_del) => {
                todo!()
            }
        }
    }

    Ok((mig, ops))
}
