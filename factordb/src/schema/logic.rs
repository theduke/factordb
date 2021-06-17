use anyhow::anyhow;

use crate::{
    backend::{DbOp, SelectOpt, TupleCreate, TupleOp},
    data::{value, Id},
    query::migrate::{Migration, SchemaAction},
    registry::Registry,
    AnyError,
};

use super::{builtin::AttrType, AttributeDescriptor};

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
                            return Err(anyhow!("Attribute upsert with a changed attribute config is not supported (yet)"));
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
                            return Err(anyhow!(
                                "Entity upsert with a changed schema is not supported (yet)"
                            ));
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
