use anyhow::anyhow;

use crate::{
    backend::{DbOp, SelectOpt, TupleCreate, TupleOp},
    data::{value, Id},
    query::migrate::{Migration, SchemaAction},
    registry::Registry,
    AnyError,
};

use super::{builtin::AttrType, AttributeDescriptor};

pub fn validate_migration(
    reg: &mut Registry,
    mut mig: Migration,
) -> Result<(Migration, Vec<DbOp>), AnyError> {
    let mut ops = Vec::new();

    for action in &mut mig.actions {
        match action {
            SchemaAction::AttributeCreate(create) => {
                create.schema.id = create.schema.id.non_nil_or_randomize();

                reg.register_attr(create.schema.clone())?;

                let mut data = value::to_value_map(create.schema.clone())?;
                // Add tye factor/type attr.
                data.insert(
                    AttrType::NAME.to_string(),
                    super::builtin::ATTRIBUTE_ID.into(),
                );

                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: create.schema.id,
                    data,
                })));
            }
            SchemaAction::AttributeUpsert(upsert) => {
                let attr = &mut upsert.schema;

                match reg.attr_by_ident(&attr.name.clone().into()) {
                    Some(old) => {
                        if !attr.id.is_nil() && attr.id != old.id {
                            return Err(anyhow!(
                                "Id mismatch: attribute name already exists with id {}",
                                old.id
                            ));
                        } else {
                            attr.id = old.id;
                        }
                        if attr != old {
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

                reg.register_attr(upsert.schema.clone())?;

                let mut data = value::to_value_map(upsert.schema.clone())?;
                // Add tye factor/type attr.
                data.insert(
                    AttrType::NAME.to_string(),
                    super::builtin::ATTRIBUTE_ID.into(),
                );

                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: upsert.schema.id,
                    data,
                })));
            }

            SchemaAction::AttributeDelete(del) => {
                let attr = reg.require_attr_by_name(&del.name)?.clone();

                let op = DbOp::Select(SelectOpt {
                    selector: crate::query::expr::Expr::literal(true),
                    op: TupleOp::RemoveAttrs(crate::backend::TupleRemoveAttrs {
                        id: Id::nil(),
                        attrs: vec![attr.id],
                    }),
                });

                ops.push(op);
            }
            SchemaAction::EntityCreate(create) => {
                create.schema.id = create.schema.id.non_nil_or_randomize();

                reg.register_entity(create.schema.clone(), true)?;

                let mut data = value::to_value_map(create.schema.clone())?;
                // Add tye factor/type attr.
                data.insert(AttrType::NAME.to_string(), super::builtin::ENTITY_ID.into());

                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: create.schema.id,
                    data,
                })));
            }
            SchemaAction::EntityUpsert(upsert) => {
                let entity = &mut upsert.schema;
                match reg.entity_by_ident(&entity.name.clone().into()) {
                    Some(old) => {
                        if !entity.id.is_nil() && entity.id != old.id {
                            return Err(anyhow!(
                                "Id mismatch: entity name already exists with id {}",
                                old.id
                            ));
                        } else {
                            entity.id = old.id;
                        }
                        if entity != old {
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
                data.insert(AttrType::NAME.to_string(), super::builtin::ENTITY_ID.into());

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
