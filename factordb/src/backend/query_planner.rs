use std::collections::HashSet;

use crate::{
    data::{Id, IdOrIdent, Value},
    query::{
        expr::{BinaryOp, Expr, UnaryOp},
        select::{self, Order, Select},
    },
    registry::{LocalAttributeId, LocalIndexId, Registry, ATTR_ID_LOCAL, ATTR_TYPE_LOCAL},
    AnyError,
};

#[derive(Debug)]
pub struct Sort<E> {
    pub on: E,
    pub order: Order,
}

#[derive(Debug)]
pub enum QueryOp<V = Value, E = Expr> {
    SelectEntity {
        id: Id,
    },
    Scan,
    Filter {
        expr: E,
    },
    Limit {
        limit: u64,
    },
    Skip {
        count: u64,
    },
    Merge {
        left: Box<Self>,
        right: Box<Self>,
    },
    IndexSelect {
        index: LocalIndexId,
        value: V,
    },
    IndexScan {
        index: LocalIndexId,
        from: Option<V>,
        until: Option<V>,
        direction: Order,
    },
    IndexScanPrefix {
        prefix: V,
    },
    Sort {
        sorts: Vec<Sort<E>>,
    },
}

#[derive(Debug)]
pub struct BinaryExpr<V> {
    pub left: ResolvedExpr<V>,
    pub op: BinaryOp,
    pub right: ResolvedExpr<V>,
}

#[derive(Debug)]
pub enum ResolvedExpr<V = Value> {
    Literal(V),
    List(Vec<Self>),
    /// Select the value of an attribute.
    Attr(LocalAttributeId),
    /// Resolve the value of an [`Ident`] into an [`Id`].
    Ident(IdOrIdent),
    UnaryOp {
        op: UnaryOp,
        expr: Box<Self>,
    },
    BinaryOp(Box<BinaryExpr<V>>),
    /// Special variant of `In` that only compares with literal values.
    /// Separated out to allow more efficient comparisons.
    InLiteral {
        value: Box<Self>,
        items: HashSet<V>,
    },
    If {
        value: Box<Self>,
        then: Box<Self>,
        or: Box<Self>,
    },
    Op(Box<QueryOp<V, ResolvedExpr<V>>>),
}

pub fn plan_select_expr(
    expr: Expr,
    reg: &Registry,
) -> Result<Vec<QueryOp<Value, ResolvedExpr>>, AnyError> {
    let resolved = resolve_expr(expr, reg)?;
    let optimized = build_select_expr(resolved);

    match optimized {
        ResolvedExpr::Op(op) => Ok(vec![*op]),
        other => Ok(vec![QueryOp::Scan, QueryOp::Filter { expr: other }]),
    }
}

pub fn plan_select(
    query: Select,
    reg: &Registry,
) -> Result<Vec<QueryOp<Value, ResolvedExpr>>, AnyError> {
    let mut ops = if let Some(filter) = query.filter {
        plan_select_expr(filter, reg)?
    } else {
        vec![QueryOp::Scan]
    };

    if !query.sort.is_empty() {
        plan_sort(reg, query.sort, &mut ops)?;
    }
    if query.offset > 0 {
        ops.push(QueryOp::Skip {
            count: query.offset,
        });
    }
    if query.limit > 0 {
        ops.push(QueryOp::Limit { limit: query.limit });
    }

    Ok(ops)
}

fn plan_sort(
    reg: &Registry,
    sorts: Vec<select::Sort>,
    ops: &mut Vec<QueryOp<Value, ResolvedExpr>>,
) -> Result<(), AnyError> {
    // Resolve the sorts.
    let sorts = sorts
        .into_iter()
        .map(|s| {
            Ok(Sort {
                on: resolve_expr(s.on, reg)?,
                order: s.order,
            })
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    // TODO:  commented out because it wouldn't work correctly yet.
    // Check if we can use an index.
    // TODO: properly handle multiple sort clauses!
    // Right now we just look at the first sort clause and sort the others
    // manually.
    // if let Some(sort) = sorts.first() {
    //     if let ResolvedExpr::Attr(attr_id) = &sort.on {
    //         // TODO: cleaner handling of index == ID ?
    //         // (memory backend is automatically sorted by ID, using the index
    //         //  would be extra overhead)

    //         if *attr_id != ATTR_ID_LOCAL {
    //             // TODO: do we need to filter the available indexes?
    //             let index_opt = reg
    //                 .indexes_for_attribute(*attr_id)
    //                 .into_iter()
    //                 .find(|_idx| true);

    //             if let Some(index) = index_opt {
    //                 ops.insert(
    //                     0,
    //                     QueryOp::IndexScan {
    //                         index: index.local_id,
    //                         from: None,
    //                         until: None,
    //                         direction: sort.order,
    //                     },
    //                 );
    //                 sorts.remove(0);
    //             }
    //         }
    //     }
    // }

    if sorts.len() > 0 {
        ops.push(QueryOp::Sort { sorts });
    }

    Ok(())
}

pub fn resolve_expr(expr: Expr, reg: &Registry) -> Result<ResolvedExpr, AnyError> {
    match expr {
        Expr::Literal(v) => Ok(ResolvedExpr::Literal(v)),
        Expr::List(items) => {
            let items = items
                .into_iter()
                .map(|e| resolve_expr(e, reg))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ResolvedExpr::List(items))
        }
        Expr::Attr(ident) => Ok(ResolvedExpr::Attr(
            reg.require_attr_by_ident(&ident)?.local_id,
        )),
        Expr::Ident(ident) => Ok(ResolvedExpr::Ident(ident)),
        Expr::Variable(_v) => Err(anyhow::anyhow!("Query variables not implemented yet")),
        Expr::UnaryOp { op, expr } => Ok(ResolvedExpr::UnaryOp {
            op,
            expr: Box::new(resolve_expr(*expr, reg)?),
        }),
        // TODO: normalize BinaryOp::In into ResolvedExpr::InLiteral if possible.
        Expr::BinaryOp { left, op, right } => Ok(ResolvedExpr::BinaryOp(Box::new(BinaryExpr {
            left: resolve_expr(*left, reg)?,
            op,
            right: resolve_expr(*right, reg)?,
        }))),
        Expr::If { value, then, or } => Ok(ResolvedExpr::If {
            value: Box::new(resolve_expr(*value, reg)?),
            then: Box::new(resolve_expr(*then, reg)?),
            or: Box::new(resolve_expr(*or, reg)?),
        }),
        Expr::InheritsEntityType(type_name) => {
            // TODO: collecting strings here is stupid and redundant.
            // Must be a cleaner way to structure this!
            // Probably want a dedicted expr to check the type!
            let ty = reg.require_entity_by_name(&type_name)?;
            let mut items: HashSet<_> = ty
                .nested_children
                .iter()
                .filter_map(|id| Some(Value::from(reg.entity_by_id(*id)?.schema.ident.clone())))
                .collect();
            items.insert(ty.schema.ident.clone().into());

            Ok(ResolvedExpr::InLiteral {
                value: Box::new(ResolvedExpr::Attr(ATTR_TYPE_LOCAL)),
                items,
            })
        }
    }
}

fn build_select_expr(expr: ResolvedExpr) -> ResolvedExpr {
    let (expr, _changed) = pass_simplify_entity_id_eq(expr);
    expr
}

/// Turn a BinaryExpr comparing a single literal Id into a direct entity select.
fn pass_simplify_entity_id_eq(expr: ResolvedExpr) -> (ResolvedExpr, bool) {
    match expr {
        ResolvedExpr::BinaryOp(binary) if binary.op == BinaryOp::Eq => {
            match (binary.left, binary.right) {
                (ResolvedExpr::Attr(ATTR_ID_LOCAL), ResolvedExpr::Literal(Value::Id(id)))
                | (ResolvedExpr::Literal(Value::Id(id)), ResolvedExpr::Attr(ATTR_ID_LOCAL)) => (
                    ResolvedExpr::Op(Box::new(QueryOp::SelectEntity { id })),
                    true,
                ),
                (left, right) => (
                    ResolvedExpr::BinaryOp(Box::new(BinaryExpr {
                        left,
                        op: binary.op,
                        right,
                    })),
                    false,
                ),
            }
        }
        other => (other, false),
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::{builtin::AttrId, AttributeDescriptor};

    use super::*;

    #[test]
    fn test_query_plan_efficient_single_entity_select() {
        let id = Id::random();
        let reg = Registry::new();
        let ops = plan_select(
            Select::new().with_filter(Expr::eq(AttrId::expr(), id)),
            &reg,
        )
        .unwrap();
        match ops.as_slice() {
            [QueryOp::SelectEntity { id: x }] => {
                assert_eq!(*x, id);
            }
            other => {
                panic!("Expected a single select, got {:?}", other);
            }
        }
    }

    /* #[test]
    fn test_query_plan_simple_sort_uses_index() {
        let reg = Registry::new();

        let ops =
            plan_select(Select::new().with_sort(AttrIdent::expr(), Order::Asc), &reg).unwrap();
        match ops.as_slice() {
            [QueryOp::IndexScan {
                index: INDEX_IDENT_LOCAL,
                direction: Order::Asc,
                from: None,
                until: None,
            }] => {
            }
            other => {
                panic!("Expected a single select, got {:?}", other);
            }
        }
    } */
}
