use std::collections::HashSet;

use anyhow::{anyhow, bail};

use crate::{
    backend::{DbOp, IndexPopulate, SelectOp, TupleDelete, TuplePatch, TupleRemoveAttrs},
    registry::Registry,
};

use factor_core::{
    data::{
        patch::Patch,
        value::{to_value, to_value_map},
        Id, IdOrIdent, Value, ValueType,
    },
    query::{
        expr::Expr,
        migrate::{self, IndexCreate, Migration, SchemaAction},
    },
    schema::{
        builtin::{self, NS_FACTOR},
        AttrMapExt, Attribute, AttributeMeta, Cardinality, ClassAttribute, IndexSchema,
    },
};

// TODO: remove allow
#[allow(dead_code)]
enum EntityAttributePatch {
    Added(ClassAttribute),
    Removed(ClassAttribute),
    CardinalityChanged {
        old: Cardinality,
        new: Cardinality,
        attribute: IdOrIdent,
    },
}

fn diff_attributes(old: &[ClassAttribute], new: &[ClassAttribute]) -> Vec<EntityAttributePatch> {
    let mut patches = Vec::new();

    for old_attr in old {
        if let Some(new_attr) = new.iter().find(|attr| attr.attribute == old_attr.attribute) {
            if old_attr.required != new_attr.required {
                patches.push(EntityAttributePatch::CardinalityChanged {
                    old: old_attr.cardinality(),
                    new: new_attr.cardinality(),
                    attribute: old_attr.attribute.clone().into(),
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

fn build_attribute_ident(attr: &Attribute) -> String {
    let unique_marker = if attr.unique { "_unique" } else { "" };
    // WARNING: DO NOT CHANGE THIS!
    // Changing this computation would be a backwards-compatability breaking
    // schema change that would break older databases.
    format!(
        "factor_indexes/attr_{}{}",
        attr.id.to_string().replace('-', "_"),
        unique_marker
    )
}

fn build_attribute_index(attr: &Attribute) -> IndexSchema {
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
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let namespace = create.schema.parse_namespace()?;
    if namespace == builtin::NS_FACTOR && !is_internal {
        return Err(anyhow!("Invalid namespace: factor/ is reserved"));
    }

    if let ValueType::RefConstrained(constr) = &create.schema.value_type {
        // Validate that the referenced entity types exist.

        for allowed in &constr.allowed_entity_types {
            let _entity_type = reg.entity_by_ident(allowed).ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid reference constraint: unknown entity type '{}'",
                    allowed
                )
            })?;
        }
    }

    // Do any necessary modifications to the schema.
    let schema = {
        let mut s = create.schema;
        s.id = s.id.non_nil_or_randomize();
        s
    };

    reg.register_attribute(schema.clone())?;

    let index_actions = if schema.index || schema.unique {
        let index = migrate::IndexCreate {
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
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let namespace = upsert.schema.parse_namespace()?;
    if namespace == builtin::NS_FACTOR && !is_internal {
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
                        let index = migrate::IndexCreate {
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

fn build_attribute_change_type(
    reg: &mut Registry,
    action: migrate::AttributeChangeType,
    _is_internal: bool,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let attr = reg.require_attr_by_name(&action.attribute)?;

    match (&attr.schema.value_type, &action.new_type) {
        // TODO: allow additional conversions.
        (ValueType::Union(old_values), ValueType::Union(new_values)) => {
            let old_set: HashSet<_> = old_values.iter().collect();
            let new_set: HashSet<_> = new_values.iter().collect();

            if !old_set.is_subset(&new_set) {
                bail!("Invalid attribute '{}' change: can't remove values from a union type, only new variants can be added", attr.schema.ident);
            }

            let mut new_schema = attr.schema.clone();
            new_schema.value_type = action.new_type.clone();
            reg.attribute_update(new_schema, true)?;

            Ok(vec![ResolvedAction {
                action: SchemaAction::AttributeChangeType(action),
                // FIXME: need an op to change the type if required!
                ops: Vec::new(),
            }])
        }
        (old, ValueType::List(item_ty)) => {
            if old == &**item_ty && !item_ty.is_list() {
                let mut new_schema = attr.schema.clone();
                new_schema.value_type = action.new_type.clone();
                reg.attribute_update(new_schema, true)?;

                Ok(vec![ResolvedAction {
                    action: SchemaAction::AttributeChangeType(action),
                    // FIXME: need an op to change the type if required!
                    ops: Vec::new(),
                }])
            } else {
                bail!("Attribute type can only be changed to list if the list items have the previous type");
            }
        }
        (old, new) => {
            bail!(
                "Changing the type of attribute '{}' from '{:?}' to '{:?}' is not supported",
                attr.schema.ident,
                old,
                new
            );
        }
    }
}

fn build_attribute_create_index(
    reg: &mut Registry,
    spec: migrate::AttributeCreateIndex,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let attr = reg.require_attr_by_name(&spec.attribute)?;
    let namespace = attr.schema.parse_namespace()?;
    if namespace == builtin::NS_FACTOR && !is_internal {
        return Err(anyhow!("Invalid namespace: factor/ is reserved"));
    }

    if attr.schema.index && attr.schema.unique {
        bail!("Attribute '{}' already has an index", spec.attribute);
    }

    let mut schema = attr.schema.clone();
    schema.index = true;
    schema.unique = spec.unique;

    let index = build_attribute_index(&schema);
    reg.register_index(index.clone())?;
    reg.attribute_update(schema, true)?;
    let mut action = ResolvedAction::new(SchemaAction::IndexCreate(IndexCreate {
        schema: index.clone(),
    }));

    action
        .ops
        .push(DbOp::IndexPopulate(IndexPopulate { index_id: index.id }));

    Ok(vec![action])
}

fn build_attribute_delete(
    reg: &mut Registry,
    del: migrate::AttributeDelete,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let attr = reg.require_attr_by_name(&del.name)?.clone();

    if attr.namespace == builtin::NS_FACTOR {
        return Err(anyhow!("Invalid namespace: factor/ is reserved"));
    }

    // Ensure that attribute is not used by any entity definition.
    for entity in reg.iter_entities() {
        for field in &entity.schema.attributes {
            let field_attr = reg.require_attr_by_name(&field.attribute)?;
            if field_attr.schema.id == attr.schema.id {
                return Err(anyhow!(
                    "Can't delete attribute '{}': still in use by entity '{}'",
                    attr.schema.ident,
                    entity.schema.ident
                ));
            }
        }
    }

    let op = DbOp::Select(SelectOp::new(
        Expr::literal(true),
        crate::backend::TupleRemoveAttrs {
            attrs: vec![attr.schema.id],
            // FIXME: handle index ops!
            index_ops: Vec::new(),
        },
    ));

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
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let namespace = create.schema.parse_namespace()?;
    if !is_internal && namespace == builtin::NS_FACTOR {
        return Err(anyhow!(
            "Invalid entity ident: the factor/ namespace is reserved"
        ));
    }

    create.schema.id = create.schema.id.non_nil_or_randomize();
    reg.register_class(create.schema.clone(), true)?;

    let action = ResolvedAction::new(SchemaAction::EntityCreate(create));
    Ok(vec![action])
}

fn build_entity_attribute_add(
    reg: &mut Registry,
    add: migrate::EntityAttributeAdd,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let attr = reg.require_attr_by_name(&add.attribute)?.clone();
    let entity = reg.require_entity_by_name_mut(&add.entity)?;

    if !is_internal && entity.schema.parse_namespace()? == NS_FACTOR {
        bail!("Can't modify builtin entitites");
    }

    if entity
        .schema
        .attributes
        .iter()
        .any(|a| a.attribute == attr.schema.ident)
    {
        bail!(
            "Entity '{}' already has the attribute '{}'",
            entity.schema.ident,
            attr.schema.ident
        );
    }

    let ops: Vec<DbOp> = if add.cardinality == Cardinality::Required {
        if let Some(value) = &add.default_value {
            // TODO: write a test that validates that nested entity types are also correctly updated.
            vec![DbOp::Select(SelectOp::new(
                Expr::InheritsEntityType(entity.schema.ident.clone()),
                TuplePatch {
                    patch: Patch::new().replace_with_old(
                        attr.schema.ident.clone(),
                        value.clone(),
                        Value::Unit,
                        false,
                    ),
                    index_ops: vec![],
                },
            ))]
        } else {
            bail!(
                "Adding attribute '{}' with required cardinality to entity '{}' requires a default value", 
                attr.schema.ident, entity.schema.ident);
        }
    } else {
        vec![]
    };

    entity.schema.attributes.push(ClassAttribute {
        attribute: attr.schema.ident.into(),
        required: add.cardinality.is_required(),
    });

    let action = ResolvedAction {
        action: SchemaAction::EntityAttributeAdd(add),
        ops,
    };
    Ok(vec![action])
}

fn build_entity_attribute_change_cardinality(
    reg: &mut Registry,
    change: migrate::EntityAttributeChangeCardinality,
    _is_internal: bool,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let attr = reg.require_attr_by_name(&change.attribute)?;
    let entity = reg.require_entity_by_name(&change.entity_type)?;

    let field = entity.schema.attribute(&change.attribute).ok_or_else(|| {
        anyhow!(
            "Can't change entity attribute cardinality: entity '{}' does not have attribute '{}'",
            entity.schema.ident,
            attr.schema.ident
        )
    })?;

    match (field.cardinality(), change.new_cardinality) {
        (Cardinality::Optional, Cardinality::Optional) => {
            bail!("Cardinality is unchanged");
        }
        (Cardinality::Optional, Cardinality::Required) => {
            // TODO: allow this change with a provided default value.
            bail!("Can't change optional fields to required");
        }
        (Cardinality::Required, Cardinality::Optional) => {}
        (Cardinality::Required, Cardinality::Required) => {
            bail!("Cardinality is unchanged");
        }
    }

    Ok(vec![ResolvedAction {
        action: migrate::SchemaAction::EntityAttributeChangeCardinality(change),
        ops: vec![],
    }])
}

fn build_entity_attribute_remove(
    reg: &mut Registry,
    change: migrate::EntityAttributeRemove,
    _is_internal: bool,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let attr = reg.require_attr_by_name(&change.attribute)?.clone();
    let entity = reg.require_entity_by_name(&change.entity_type)?.clone();

    let _field = entity.schema.attribute(&change.attribute).ok_or_else(|| {
        anyhow!(
            "Can't remove attribute from entity: '{}' does not have attribute '{}'",
            entity.schema.ident,
            attr.schema.ident
        )
    })?;

    let mut new_entity = entity.schema.clone();
    new_entity
        .attributes
        .retain(|a| a.attribute != attr.schema.ident);
    reg.update_class(new_entity, true)?;

    let ops = if change.delete_values {
        vec![DbOp::Select(SelectOp::new(
            Expr::is_entity_name(&entity.schema.ident),
            TupleRemoveAttrs {
                attrs: vec![attr.schema.id],
                // FIXME: update index!
                index_ops: vec![],
            },
        ))]
    } else {
        vec![]
    };

    Ok(vec![ResolvedAction {
        action: migrate::SchemaAction::EntityAttributeRemove(change),
        ops,
    }])
}

fn build_entity_upsert(
    reg: &mut Registry,
    upsert: migrate::EntityUpsert,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let namespace = upsert.schema.parse_namespace()?;
    if !is_internal && namespace == builtin::NS_FACTOR {
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
                    if !new_attr.required {
                        new_attrs.push(new_attr);
                    } else {
                        bail!(
                            "Entity upsert with new attribute '{:?}' is invalid - new attributes must be optional",
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
            builtin::AttrClassAttributes::QUALIFIED_NAME.into(),
            new_attrs_value,
        );
    }

    reg.update_class(schema.clone(), true)?;

    let action = ResolvedAction::new(SchemaAction::EntityUpsert(migrate::EntityUpsert { schema }));
    Ok(vec![action])
}

fn build_entity_delete(
    reg: &mut Registry,
    del: migrate::EntityDelete,
    _is_internal: bool,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let schema = reg.require_entity_by_name(&del.name)?;

    let ops = if del.delete_all {
        vec![DbOp::Select(SelectOp::new(
            Expr::is_entity_name(&schema.schema.ident),
            TupleDelete {
                // FIXME: need to build index updates here!
                index_ops: vec![],
            },
        ))]
    } else {
        vec![]
    };

    let action = ResolvedAction {
        action: SchemaAction::EntityDelete(del),
        ops,
    };

    Ok(vec![action])
}

fn build_index_create(
    reg: &mut Registry,
    mut create: migrate::IndexCreate,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
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
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    let id = reg.require_index_by_name(&del.name)?.schema.id;
    reg.remove_index(id)?;

    let action = ResolvedAction::new(SchemaAction::IndexDelete(del));
    Ok(vec![action])
}

fn build_action(
    reg: &mut Registry,
    action: SchemaAction,
    is_internal: bool,
) -> Result<Vec<ResolvedAction>, anyhow::Error> {
    match action {
        SchemaAction::AttributeCreate(create) => build_attribute_create(reg, create, is_internal),
        SchemaAction::AttributeUpsert(upsert) => build_attribute_upsert(reg, upsert, is_internal),
        SchemaAction::AttributeChangeType(a) => build_attribute_change_type(reg, a, is_internal),
        SchemaAction::AttributeCreateIndex(spec) => {
            build_attribute_create_index(reg, spec, is_internal)
        }
        SchemaAction::AttributeDelete(del) => build_attribute_delete(reg, del),
        SchemaAction::EntityCreate(create) => build_entity_create(reg, create, is_internal),
        SchemaAction::EntityAttributeAdd(add) => build_entity_attribute_add(reg, add, is_internal),
        SchemaAction::EntityAttributeChangeCardinality(change) => {
            build_entity_attribute_change_cardinality(reg, change, is_internal)
        }
        SchemaAction::EntityAttributeRemove(rem) => {
            build_entity_attribute_remove(reg, rem, is_internal)
        }
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
) -> Result<(Migration, Vec<DbOp>), anyhow::Error> {
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
