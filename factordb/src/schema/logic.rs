use anyhow::{anyhow, bail};

use crate::{
    backend::{DbOp, SelectOpt, TupleOp, TuplePatch},
    data::{
        value::{patch::Patch, to_value, to_value_map},
        Id,
    },
    query::{
        expr::Expr,
        migrate::{self, Migration, SchemaAction},
    },
    registry::Registry,
    schema::{
        builtin::{self, NS_FACTOR},
        AttrMapExt,
    },
    AnyError, Ident, Value,
};

use super::{AttributeDescriptor, AttributeSchema, Cardinality, EntityAttribute, IndexSchema};

// TODO: remove allow
#[allow(dead_code)]
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

fn build_attribute_ident(attr: &AttributeSchema) -> String {
    let unique_marker = if attr.unique { "_unique" } else { "" };
    // WARNING: DO NOT CHANGE THIS!
    // Changing this computation would be a backwards-compatability breaking
    // schema change that would break older databases.
    format!(
        "factor_indexes/attr_{}{}",
        attr.id.to_string().replace("-", "_"),
        unique_marker
    )
}

fn build_attribute_index(attr: &AttributeSchema) -> IndexSchema {
    IndexSchema {
        id: Id::random(),
        ident: build_attribute_ident(attr),
        title: None,
        attributes: vec![attr.id],
        description: None,
        unique: attr.unique,
    }
}

struct ResolvedAction {
    action: SchemaAction,
    ops: Vec<DbOp>,
}

impl ResolvedAction {
    fn new(action: SchemaAction) -> Self {
        Self {
            action,
            ops: Vec::new(),
        }
    }
}

fn build_attribute_create(
    reg: &mut Registry,
    create: migrate::AttributeCreate,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, AnyError> {
    let namespace = create.schema.parse_namespace()?;
    if namespace == super::builtin::NS_FACTOR && !is_internal {
        return Err(anyhow!("Invalid namespace: factor/ is reserved"));
    }

    // Do any necessary modifications to the schema.
    let schema = {
        let mut s = create.schema;
        s.id = s.id.non_nil_or_randomize();
        s
    };

    reg.register_attribute(schema.clone())?;

    let index_actions = if schema.index || schema.unique {
        let index = crate::query::migrate::IndexCreate {
            schema: build_attribute_index(&schema),
        };
        build_index_create(reg, index)?
    } else {
        vec![]
    };

    let action = ResolvedAction::new(SchemaAction::AttributeCreate(migrate::AttributeCreate {
        schema,
    }));

    let mut actions = vec![action];

    actions.extend(index_actions);

    Ok(actions)
}

fn build_attribute_upsert(
    reg: &mut Registry,
    upsert: migrate::AttributeUpsert,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, AnyError> {
    let namespace = upsert.schema.parse_namespace()?;
    if namespace == super::builtin::NS_FACTOR && !is_internal {
        return Err(anyhow!("Invalid namespace: factor/ is reserved"));
    }

    let mut schema = upsert.schema;

    match reg.attr_by_ident(&schema.ident.clone().into()) {
        None => build_attribute_create(reg, migrate::AttributeCreate { schema }, is_internal),
        Some(old) => {
            if !schema.id.is_nil() && schema.id != old.schema.id {
                bail!(
                    "Id mismatch: attribute name already exists with id {}",
                    old.schema.id
                );
            } else {
                schema.id = old.schema.id;
            }

            if schema != old.schema {
                bail!(
                    "Attribute '{}' has changed - upsert with a changed attribute schema is not supported (yet)\n\nold: {:?}\n\n new: {:?}", 
                    schema.ident,
                    old,
                    schema,
                );
            }

            // Make sure index exists.
            // This is here to support databases created before
            // index creation was supported.
            if schema.unique || schema.index {
                let index_schema = build_attribute_index(&schema);

                match reg.index_by_name(&index_schema.ident) {
                    Some(old) => {
                        if old.schema != index_schema {
                            // TODO: figure out how to handle this here,
                            // and check for allowed changes. any changes here
                            // would be due to an internal change to attribute
                            // index creation.
                            bail!("Invalid attribute upsert with index: new index schema would be incompatible");
                        }
                        Ok(vec![])
                    }
                    None => {
                        let index = crate::query::migrate::IndexCreate {
                            schema: index_schema,
                        };
                        let action = build_index_create(reg, index)?;
                        Ok(action)
                    }
                }
            } else {
                Ok(vec![])
            }
        }
    }
}

fn build_attribute_delete(
    reg: &mut Registry,
    del: migrate::AttributeDelete,
) -> Result<Vec<ResolvedAction>, AnyError> {
    let attr = reg.require_attr_by_name(&del.name)?.clone();

    if attr.namespace == super::builtin::NS_FACTOR {
        return Err(anyhow!("Invalid namespace: factor/ is reserved"));
    }

    // Ensure that attribute is not used by any entity definition.
    for entity in reg.iter_entities() {
        for field in &entity.schema.attributes {
            let field_attr = reg.require_attr_by_ident(&field.attribute)?;
            if field_attr.schema.id == attr.schema.id {
                return Err(anyhow!(
                    "Can't delete attribute '{}': still in use by entity '{}'",
                    attr.schema.ident,
                    entity.schema.ident
                ));
            }
        }
    }

    let op = DbOp::Select(SelectOpt {
        selector: crate::query::expr::Expr::literal(true),
        op: TupleOp::RemoveAttrs(crate::backend::TupleRemoveAttrs {
            id: attr.schema.id,
            attrs: vec![attr.schema.id],
            // FIXME: handle index ops!
            index_ops: Vec::new(),
        }),
    });

    let action = ResolvedAction {
        action: SchemaAction::AttributeDelete(del),
        ops: vec![op],
    };

    Ok(vec![action])
}

fn build_entity_create(
    reg: &mut Registry,
    mut create: migrate::EntityCreate,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, AnyError> {
    let namespace = create.schema.parse_namespace()?;
    if !is_internal && namespace == super::builtin::NS_FACTOR {
        return Err(anyhow!(
            "Invalid entity ident: the factor/ namespace is reserved"
        ));
    }

    create.schema.id = create.schema.id.non_nil_or_randomize();
    reg.register_entity(create.schema.clone(), true)?;

    let action = ResolvedAction::new(SchemaAction::EntityCreate(create));
    Ok(vec![action])
}

fn build_entity_attribute_add(
    reg: &mut Registry,
    add: migrate::EntityAttributeAdd,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, AnyError> {
    let attr = reg.require_attr_by_name(&add.attribute)?;
    let mut entity = reg
        .entity_by_name(&add.entity)
        .ok_or_else(|| anyhow!("Entity type '{}' does not exist", add.entity))?
        .schema
        .clone();

    if !is_internal && entity.parse_namespace()? == NS_FACTOR {
        bail!("Can't modify builtin entitites");
    }

    if entity
        .attributes
        .iter()
        .any(|a| a.attribute == attr.schema.id.into())
    {
        bail!(
            "Entity '{}' already has the attribute '{}'",
            entity.ident,
            attr.schema.ident
        );
    }

    let ops: Vec<DbOp> = if add.cardinality == Cardinality::Required {
        if let Some(value) = &add.default_value {
            // TODO: write a test that validates that nested entity types are also correctly updated.
            vec![DbOp::Select(SelectOpt {
                selector: Expr::InheritsEntityType(entity.ident.clone()),
                op: TupleOp::Patch(TuplePatch {
                    id: Id::nil(),
                    patch: Patch::new().replace_with_old(
                        attr.schema.ident.clone(),
                        value.clone(),
                        Value::Unit,
                        false,
                    ),
                    index_ops: vec![],
                }),
            })]
        } else {
            bail!(
                "Adding attribute '{}' with required cardinality to entity '{}' requires a default value", 
                attr.schema.ident, entity.ident);
        }
    } else {
        vec![]
    };

    entity.attributes.push(EntityAttribute {
        attribute: attr.schema.id.into(),
        cardinality: add.cardinality,
    });

    let action = ResolvedAction {
        action: SchemaAction::EntityAttributeAdd(add),
        ops,
    };
    Ok(vec![action])
}

fn build_entity_upsert(
    reg: &mut Registry,
    upsert: migrate::EntityUpsert,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, AnyError> {
    let namespace = upsert.schema.parse_namespace()?;
    if !is_internal && namespace == super::builtin::NS_FACTOR {
        bail!("Invalid entity ident: the factor/ namespace is reserved");
    }

    let old = match reg.entity_by_ident(&upsert.schema.ident.clone().into()) {
        Some(old) => old,
        None => {
            // Entity does not exist yet, so just create it.
            return build_entity_create(
                reg,
                migrate::EntityCreate {
                    schema: upsert.schema,
                },
                is_internal,
            );
        }
    };

    // Entity already exists, so check for modifications.

    let mut schema = upsert.schema;
    if !schema.id.is_nil() && schema.id != old.schema.id {
        bail!(
            "Id mismatch: entity name already exists with id {}",
            old.schema.id
        );
    }
    schema.id = old.schema.id;

    if schema == old.schema {
        // Entity has not changed, nothing to do.
        return Ok(vec![]);
    }

    // Entity has changed.
    // Check what has changed and if we can allow an upsert.

    if old.schema.ident != schema.ident {
        bail!("Entity upsert with changed ident is not supported");
    }
    if old.schema.extends != schema.extends {
        bail!("Entity upsert with changed extend parent schemas is not supported");
    }
    if old.schema.strict != schema.strict {
        bail!("Entity upsert with changed strict setting is not supported");
    }

    let mut merge = to_value_map(&old.schema)?;

    if old.schema.title != schema.title {
        if let Some(new_title) = &schema.title {
            merge.insert_attr::<builtin::AttrTitle>(new_title.clone());
        } else {
            merge.remove(builtin::AttrTitle::QUALIFIED_NAME);
        }
    }

    if old.schema.description != schema.description {
        if let Some(new_description) = &schema.description {
            merge.insert_attr::<builtin::AttrDescription>(new_description.clone());
        } else {
            merge.remove(builtin::AttrDescription::QUALIFIED_NAME);
        }
    }

    if old.schema.attributes != schema.attributes {
        let diffs = diff_attributes(&old.schema.attributes, &schema.attributes);

        let mut new_attrs = old.schema.attributes.clone();

        for diff in diffs {
            match diff {
                EntityAttributePatch::Added(new_attr) => {
                    if new_attr.cardinality != Cardinality::Required {
                        new_attrs.push(new_attr);
                    } else {
                        bail!(
                            "Entity upsert with new attribute '{:?}' is invalid - new attributes must have a cardinality of Optional or Many",
                            new_attr.attribute,
                        );
                    }
                }
                EntityAttributePatch::Removed(removed) => {
                    bail!(
                        "Entity upsert can not remove attributes (attribute '{:?}')",
                        removed.attribute,
                    );
                }
                EntityAttributePatch::CardinalityChanged {
                    old: _,
                    new: _,
                    attribute,
                } => {
                    bail!(
                        "Entity upsert can not change attribute cardinality (attribute '{:?}')",
                        attribute,
                    );
                }
            }
        }

        let new_attrs_value = to_value(new_attrs)?;
        merge.insert(
            builtin::AttrAttributes::QUALIFIED_NAME.into(),
            new_attrs_value,
        );
    }

    reg.entity_update(schema.clone(), true)?;

    let action = ResolvedAction::new(SchemaAction::EntityUpsert(migrate::EntityUpsert { schema }));
    Ok(vec![action])
}

fn build_entity_delete(
    _reg: &mut Registry,
    _del: migrate::EntityDelete,
    _is_internal: bool,
) -> Result<Vec<ResolvedAction>, AnyError> {
    Err(anyhow!("Entity deletion is not implemented yet"))
}

fn build_index_create(
    reg: &mut Registry,
    mut create: migrate::IndexCreate,
) -> Result<Vec<ResolvedAction>, AnyError> {
    // FIXME: validate namespace
    // let namespace = create.schema.parse_namespace()?;
    // if namespace == super::builtin::NS_FACTOR && !is_internal {
    //     return Err(anyhow!("Invalid namespace: factor/ is reserved"));
    // }

    create.schema.id = create.schema.id.non_nil_or_randomize();
    reg.register_index(create.schema.clone())?;
    let action = ResolvedAction::new(SchemaAction::IndexCreate(create));
    Ok(vec![action])
}

fn build_index_delete(
    reg: &mut Registry,
    del: migrate::IndexDelete,
) -> Result<Vec<ResolvedAction>, AnyError> {
    let id = reg.require_index_by_name(&del.name)?.schema.id;
    reg.remove_index(id)?;

    let action = ResolvedAction::new(SchemaAction::IndexDelete(del));
    Ok(vec![action])
}

fn build_action(
    reg: &mut Registry,
    action: SchemaAction,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, AnyError> {
    match action {
        SchemaAction::AttributeCreate(create) => build_attribute_create(reg, create, is_internal),
        SchemaAction::AttributeUpsert(upsert) => build_attribute_upsert(reg, upsert, is_internal),
        SchemaAction::AttributeDelete(del) => build_attribute_delete(reg, del),
        SchemaAction::EntityCreate(create) => build_entity_create(reg, create, is_internal),
        SchemaAction::EntityAttributeAdd(add) => build_entity_attribute_add(reg, add, is_internal),
        SchemaAction::EntityUpsert(upsert) => build_entity_upsert(reg, upsert, is_internal),
        SchemaAction::EntityDelete(del) => build_entity_delete(reg, del, is_internal),
        SchemaAction::IndexCreate(create) => build_index_create(reg, create),
        SchemaAction::IndexDelete(del) => build_index_delete(reg, del),
    }
}

/// Validate a migration against the registry.
///
/// NOTE: is_internal must be set to false for regular migrations, and to true
/// for internal migrations driven by the factor db.
/// With is_internal = false, any changes to builtin entities/attributes are
/// rejected.
pub fn build_migration(
    reg: &mut Registry,
    mut mig: Migration,
    is_internal: bool,
) -> Result<(Migration, Vec<DbOp>), AnyError> {
    let mut actions = Vec::new();
    let mut ops = Vec::new();

    for action in mig.actions {
        let resolved = build_action(reg, action, is_internal)?;
        for sub_action in resolved {
            actions.push(sub_action.action);
            ops.extend(sub_action.ops);
        }
    }
    mig.actions = actions;

    Ok((mig, ops))
}
