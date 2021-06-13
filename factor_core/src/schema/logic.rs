use crate::{
    backend::{DbOp, TupleCreate, TupleOp},
    data::{value, Id},
    query::migrate::{Migration, SchemaAction},
    registry::Registry,
    AnyError,
};

pub fn validate_migration(
    reg: &mut Registry,
    mut mig: Migration,
) -> Result<(Migration, Vec<DbOp>), AnyError> {
    let mut ops = Vec::new();

    for action in &mut mig.actions {
        match action {
            SchemaAction::AttributeCreate(create) => {
                if create.schema.id.is_nil() {
                    create.schema.id = Id::random();
                }

                reg.register_attr(create.schema.clone())?;

                let data = value::to_value_map(create.schema.clone())?;
                ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
                    id: create.schema.id,
                    data,
                })));
            }
            SchemaAction::AttributeDelete(_del) => {
                todo!()
            }
            SchemaAction::EntityCreate(create) => {
                if create.schema.id.is_nil() {
                    create.schema.id = Id::random();
                }

                reg.register_entity(create.schema.clone(), true)?;

                let data = value::to_value_map(create.schema.clone())?;
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
