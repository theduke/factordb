use crate::{
    backend::{DbOp, TupleCreate, TupleOp},
    data::{self, value},
    query::migrate::{Migration, SchemaAction},
    registry::Registry,
    AnyError,
};

use super::{builtin::AttrType, AttributeDescriptor, EntityDescriptor};

pub fn validate_migration(
    reg: &mut Registry,
    mut mig: Migration,
) -> Result<(Migration, Vec<DbOp>), AnyError> {
    let mut ops = Vec::new();

    for action in &mut mig.actions {
        match action {
            SchemaAction::AttributeCreate(create) => {
                create.schema.id = create.schema.id.into_non_nil();

                reg.register_attr(create.schema.clone())?;

                let mut data = value::to_value_map(create.schema.clone())?;
                // Add tye factor/type attr.
                data.insert(
                    AttrType::NAME.to_string(),
                    super::builtin::AttributeSchemaType::ID.into(),
                );

                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: create.schema.id,
                    data,
                })));
            }
            SchemaAction::AttributeDelete(del) => {
                let attr = reg.require_attr_by_name(&del.name)?.clone();

                let mut patch_data = data::DataMap::new();
                // TODO: better handling of removal than with a Unit replace?
                patch_data.insert(attr.name.to_string(), data::Value::Unit);

                let op = DbOp::Tuple(TupleOp::SelectRemove(crate::backend::TupleSelectRemove {
                    selector: crate::query::expr::Expr::literal(true),
                    attrs: vec![attr.id],
                }));

                ops.push(op);
            }
            SchemaAction::EntityCreate(create) => {
                create.schema.id = create.schema.id.into_non_nil();

                reg.register_entity(create.schema.clone(), true)?;

                let mut data = value::to_value_map(create.schema.clone())?;
                // Add tye factor/type attr.
                data.insert(
                    AttrType::NAME.to_string(),
                    super::builtin::EntitySchemaType::ID.into(),
                );

                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: create.schema.id,
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
