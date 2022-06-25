mod expr_optimize;
mod optimizers;

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

use crate::registry::{LocalAttributeId, LocalIndexId, Registry, ATTR_TYPE_LOCAL};

use self::{expr_optimize::OwnedExprOptimizer, optimizers::FalliblePlanOptimizer};

#[derive(Clone, Debug, PartialEq, Eq)]
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

pub enum RecursionResult<T> {
    Some(T),
    None,
    Abort,
}

impl<V: Clone, E: Clone> QueryPlan<V, E> {
    /// Recursive map a [`QueryPlan`], allowing the provided mapper function to
    /// optionally return a modified nested plan.
    pub fn map_recurse_abortable(&self, f: fn(&Self) -> RecursionResult<Self>) -> Self {
        match f(self) {
            RecursionResult::Some(v) => {
                return v;
            }
            RecursionResult::Abort => {
                return self.clone();
            }
            RecursionResult::None => {}
        }

        match self {
            Self::EmptyRelation => Self::EmptyRelation,
            Self::SelectEntity { .. } => self.clone(),
            Self::Scan { .. } => self.clone(),
            Self::Filter { expr, input } => Self::Filter {
                expr: expr.clone(),
                input: Box::new(input.map_recurse_abortable(f)),
            },
            Self::Limit { limit, input } => Self::Limit {
                limit: *limit,
                input: Box::new(input.map_recurse_abortable(f)),
            },
            Self::Skip { count, input } => Self::Skip {
                count: *count,
                input: Box::new(input.map_recurse_abortable(f)),
            },
            Self::Merge { left, right } => Self::Merge {
                left: Box::new(left.map_recurse_abortable(f)),
                right: Box::new(right.map_recurse_abortable(f)),
            },
            Self::IndexSelect { .. } => self.clone(),
            Self::IndexScan { .. } => self.clone(),
            Self::IndexScanPrefix { .. } => self.clone(),
            Self::Sort { sorts, input } => Self::Sort {
                sorts: sorts.clone(),
                input: Box::new(input.map_recurse_abortable(f)),
            },
        }
    }

    /// Recursive map a [`QueryPlan`], allowing the provided mapper function to
    /// optionally return a modified nested plan.
    pub fn map_recurse<F: Fn(&Self) -> Option<Self>>(&self, f: F) -> Option<Self> {
        if let Some(new) = f(self) {
            Some(new)
        } else {
            match self {
                Self::EmptyRelation => None,
                Self::SelectEntity { .. } => None,
                Self::Scan { .. } => None,
                Self::Filter { expr, input } => Some(Self::Filter {
                    expr: expr.clone(),
                    input: Box::new(input.map_recurse(f)?),
                }),
                Self::Limit { limit, input } => Some(Self::Limit {
                    limit: *limit,
                    input: Box::new(input.map_recurse(f)?),
                }),
                Self::Skip { count, input } => Some(Self::Skip {
                    count: *count,
                    input: Box::new(input.map_recurse(f)?),
                }),
                Self::Merge { left, right } => {
                    if let Some(x) = f(&left) {
                        Some(Self::Merge {
                            left: Box::new(x),
                            right: right.clone(),
                        })
                    } else if let Some(x) = f(right) {
                        Some(Self::Merge {
                            left: left.clone(),
                            right: Box::new(x),
                        })
                    } else {
                        None
                    }
                }
                Self::IndexSelect { .. } => None,
                Self::IndexScan { .. } => None,
                Self::IndexScanPrefix { .. } => None,
                Self::Sort { sorts, input } => Some(Self::Sort {
                    sorts: sorts.clone(),
                    input: Box::new(input.map_recurse(f)?),
                }),
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

impl<V: PartialEq + Eq + std::hash::Hash> PartialEq for BinaryExpr<V> {
    fn eq(&self, other: &Self) -> bool {
        self.left == other.left && self.op == other.op && self.right == other.right
    }
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
}

impl<V> ResolvedExpr<V> {
    pub fn literal<T: Into<V>>(x: T) -> Self {
        Self::Literal(x.into())
    }

    pub fn binary(left: Self, op: BinaryOp, right: Self) -> Self {
        Self::BinaryOp(Box::new(BinaryExpr { left, op, right }))
    }

    pub fn eq(left: Self, right: Self) -> Self {
        Self::binary(left, BinaryOp::Eq, right)
    }

    pub fn and(left: Self, right: Self) -> Self {
        Self::binary(left, BinaryOp::And, right)
    }

    pub fn as_literal(&self) -> Option<&V> {
        if let Self::Literal(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl<V: PartialEq + Eq + std::hash::Hash> PartialEq for ResolvedExpr<V> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Literal(l0), Self::Literal(r0)) => l0 == r0,
            (Self::Regex(l0), Self::Regex(r0)) => l0.as_str() == r0.as_str(),
            (Self::List(l0), Self::List(r0)) => l0 == r0,
            (Self::Attr(l0), Self::Attr(r0)) => l0 == r0,
            (Self::Ident(l0), Self::Ident(r0)) => l0 == r0,
            (
                Self::UnaryOp {
                    op: l_op,
                    expr: l_expr,
                },
                Self::UnaryOp {
                    op: r_op,
                    expr: r_expr,
                },
            ) => l_op == r_op && l_expr == r_expr,
            (Self::BinaryOp(l0), Self::BinaryOp(r0)) => l0 == r0,
            (
                Self::InLiteral {
                    value: l_value,
                    items: l_items,
                },
                Self::InLiteral {
                    value: r_value,
                    items: r_items,
                },
            ) => l_value == r_value && *l_items == *r_items,
            (
                Self::If {
                    value: l_value,
                    then: l_then,
                    or: l_or,
                },
                Self::If {
                    value: r_value,
                    then: r_then,
                    or: r_or,
                },
            ) => l_value == r_value && l_then == r_then && l_or == r_or,
            _ => false,
        }
    }
}

impl<V: Eq + std::hash::Hash> Eq for ResolvedExpr<V> {}

impl<V> ResolvedExpr<V> {
    pub fn as_binary_op(&self) -> Option<&BinaryExpr<V>> {
        if let Self::BinaryOp(v) = self {
            Some(&v)
        } else {
            None
        }
    }

    pub fn as_binary_op_with_op(&self, op: BinaryOp) -> Option<(&Self, &Self)> {
        let bin = self.as_binary_op()?;
        if bin.op == op {
            Some((&bin.left, &bin.right))
        } else {
            None
        }
    }

    pub fn as_binary_op_and(&self) -> Option<(&Self, &Self)> {
        self.as_binary_op_with_op(BinaryOp::And)
    }

    pub fn as_binary_op_in(&self) -> Option<(&Self, &Self)> {
        self.as_binary_op_with_op(BinaryOp::In)
    }

    pub fn as_binary_op_eq(&self) -> Option<(&Self, &Self)> {
        self.as_binary_op_with_op(BinaryOp::Eq)
    }

    pub fn as_binary_op_attr_eq_value(&self) -> Option<(LocalAttributeId, &V)> {
        match self.as_binary_op_eq()? {
            (ResolvedExpr::Attr(id), ResolvedExpr::Literal(v)) => Some((*id, v.clone())),
            (ResolvedExpr::Literal(v), ResolvedExpr::Attr(id)) => Some((*id, v.clone())),
            _ => None,
        }
    }

    pub fn as_in_literal_attr(&self) -> Option<(LocalAttributeId, &HashSet<V>)> {
        match self {
            ResolvedExpr::InLiteral { value, items } => {
                let attr = value.as_attr()?;
                Some((*attr, items))
            }
            _ => None,
        }
    }

    pub fn as_attr(&self) -> Option<&LocalAttributeId> {
        if let Self::Attr(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

fn optimize_expr(expr: ResolvedExpr) -> ResolvedExpr {
    expr_optimize::BinaryToInLiteral.optimize(expr)
}

pub fn plan_select(
    query: Select,
    reg: &Registry,
) -> Result<QueryPlan<Value, ResolvedExpr>, AnyError> {
    let filter_unoptimized = query
        .filter
        .clone()
        .map(|e| resolve_expr(e, reg))
        .transpose()?;
    let filter = filter_unoptimized.map(optimize_expr);

    let plan = Box::new(QueryPlan::<Value, ResolvedExpr>::Scan { filter });

    let plan = if !query.sort.is_empty() {
        let sorts = plan_sort(reg, query.sort.clone())?;
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

    let optimizers: Vec<&dyn FalliblePlanOptimizer> = vec![
        &optimizers::OptimizeEntitySelect,
        &optimizers::FilterWithIndex,
    ];

    let plan = optimizers.iter().try_fold(
        *plan,
        |plan, opt| -> Result<QueryPlan<Value, ResolvedExpr>, anyhow::Error> {
            if let Some(new) = opt.optimize(reg, &plan)? {
                Ok(new)
            } else {
                Ok(plan)
            }
        },
    )?;

    tracing::debug!(?query, ?plan, "planned select query");

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
