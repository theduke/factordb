use std::collections::HashSet;

use anyhow::Context;

use factordb::{
    data::{Id, IdOrIdent, Value},
    query::{
        expr::{BinaryOp, Expr, UnaryOp},
        select::{self, Order, Select},
    },
    AnyError,
};

use crate::registry::{LocalAttributeId, LocalIndexId, Registry, ATTR_ID_LOCAL, ATTR_TYPE_LOCAL};

#[derive(Clone, Debug)]
pub enum QueryPlan<V = Value, E = Expr> {
    /// Empty set of tuples.
    /// Useful for optimization passes.
    EmptyRelation,
    SelectEntity {
        id: Id,
    },
    Scan {
        filter: Option<E>,
    },
    Filter {
        expr: E,

        input: Box<Self>,
    },
    Limit {
        limit: u64,

        input: Box<Self>,
    },
    Skip {
        count: u64,

        input: Box<Self>,
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
        index: LocalIndexId,
        direction: Order,
        prefix: V,
    },
    Sort {
        sorts: Vec<Sort<E>>,

        input: Box<Self>,
    },
}

impl<V: Clone, E: Clone> QueryPlan<V, E> {
    /// Recursive map a [`QueryPlan`], allowing the provided mapper function to
    /// optionally return a modified nested plan.
    fn map_recurse(&self, f: fn(&Self) -> Option<Self>) -> Self {
        if let Some(new) = f(self) {
            new
        } else {
            match self {
                Self::EmptyRelation => Self::EmptyRelation,
                Self::SelectEntity { .. } => self.clone(),
                Self::Scan { .. } => self.clone(),
                Self::Filter { expr, input } => Self::Filter {
                    expr: expr.clone(),
                    input: Box::new(input.map_recurse(f)),
                },
                Self::Limit { limit, input } => Self::Limit {
                    limit: *limit,
                    input: Box::new(input.map_recurse(f)),
                },
                Self::Skip { count, input } => Self::Skip {
                    count: *count,
                    input: Box::new(input.map_recurse(f)),
                },
                Self::Merge { left, right } => Self::Merge {
                    left: Box::new(left.map_recurse(f)),
                    right: Box::new(right.map_recurse(f)),
                },
                Self::IndexSelect { .. } => self.clone(),
                Self::IndexScan { .. } => self.clone(),
                Self::IndexScanPrefix { .. } => self.clone(),
                Self::Sort { sorts, input } => Self::Sort {
                    sorts: sorts.clone(),
                    input: Box::new(input.map_recurse(f)),
                },
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Sort<E> {
    pub on: E,
    pub order: Order,
}

#[derive(Clone, Debug)]
pub struct BinaryExpr<V> {
    pub left: ResolvedExpr<V>,
    pub op: BinaryOp,
    pub right: ResolvedExpr<V>,
}

#[derive(Clone, Debug)]
pub enum ResolvedExpr<V = Value> {
    Literal(V),
    Regex(regex::Regex),
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
    Op(Box<QueryPlan<V, ResolvedExpr<V>>>),
}

pub fn plan_select(
    query: Select,
    reg: &Registry,
) -> Result<QueryPlan<Value, ResolvedExpr>, AnyError> {
    let filter = query.filter.map(|e| resolve_expr(e, reg)).transpose()?;
    let plan = Box::new(QueryPlan::<Value, ResolvedExpr>::Scan { filter });

    let plan = if !query.sort.is_empty() {
        let sorts = plan_sort(reg, query.sort)?;
        Box::new(QueryPlan::Sort { sorts, input: plan })
    } else {
        plan
    };

    let plan = if query.offset > 0 {
        Box::new(QueryPlan::Skip {
            count: query.offset,
            input: plan,
        })
    } else {
        plan
    };

    let plan = if query.limit > 0 {
        Box::new(QueryPlan::Limit {
            limit: query.limit,
            input: plan,
        })
    } else {
        plan
    };

    // run optimizers.

    let optimizers: Vec<&dyn Optimizer> = vec![&OptimizeEntitySelect];

    let plan = optimizers.iter().try_fold(
        *plan,
        |plan, opt| -> Result<QueryPlan<Value, ResolvedExpr>, anyhow::Error> {
            if let Some(new) = opt.optimize(&plan)? {
                Ok(new)
            } else {
                Ok(plan)
            }
        },
    )?;

    Ok(plan)
}

fn plan_sort(
    reg: &Registry,
    sorts: Vec<select::Sort>,
) -> Result<Vec<Sort<ResolvedExpr>>, AnyError> {
    sorts
        .into_iter()
        .map(|s| {
            Ok(Sort {
                on: resolve_expr(s.on, reg)?,
                order: s.order,
            })
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()
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
        Expr::BinaryOp {
            left,
            op: BinaryOp::RegexMatch,
            right,
        } => {
            let raw = right.as_literal().and_then(|v| v.as_str()).ok_or_else(|| {
                anyhow::anyhow!("Invalid binary expr RegexMatch: right operand must be a string")
            })?;
            let re = regex::Regex::new(raw).context("Invalid regular expression")?;
            Ok(ResolvedExpr::BinaryOp(Box::new(BinaryExpr {
                left: resolve_expr(*left, reg)?,
                op: BinaryOp::RegexMatch,
                right: ResolvedExpr::Regex(re),
            })))
        }
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

trait Optimizer {
    fn optimize(
        &self,
        plan: &QueryPlan<Value, ResolvedExpr>,
    ) -> Result<Option<QueryPlan<Value, ResolvedExpr>>, anyhow::Error>;
}

struct OptimizeEntitySelect;

impl Optimizer for OptimizeEntitySelect {
    fn optimize(
        &self,
        plan: &QueryPlan<Value, ResolvedExpr>,
    ) -> Result<Option<QueryPlan<Value, ResolvedExpr>>, anyhow::Error> {
        let new = plan.map_recurse(|plan| match plan {
            QueryPlan::Scan { filter } => {
                if let Some(id) = filter.as_ref().and_then(expr_is_entity_id_eq) {
                    Some(QueryPlan::SelectEntity { id })
                } else {
                    None
                }
            }
            // TODO: handle higher level filter also?
            // QueryPlan::Filter { expr, input } => todo!(),
            _ => None,
        });

        Ok(Some(new))
    }
}

fn expr_is_entity_id_eq(expr: &ResolvedExpr) -> Option<Id> {
    match expr {
        ResolvedExpr::BinaryOp(binary) if binary.op == BinaryOp::Eq => {
            match (&binary.left, &binary.right) {
                (ResolvedExpr::Attr(ATTR_ID_LOCAL), ResolvedExpr::Literal(Value::Id(id)))
                | (ResolvedExpr::Literal(Value::Id(id)), ResolvedExpr::Attr(ATTR_ID_LOCAL)) => {
                    Some(*id)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use factordb::schema::{builtin::AttrId, AttributeDescriptor};

    use super::*;

    #[test]
    fn test_query_plan_efficient_single_entity_select() {
        let id = Id::random();
        let reg = Registry::new();
        let plan = plan_select(
            Select::new().with_filter(Expr::eq(AttrId::expr(), id)),
            &reg,
        )
        .unwrap();

        match plan {
            QueryPlan::SelectEntity { id: id2 } if id == id2 => {}
            other => panic!("expected SelectEntity, got {other:?}"),
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
