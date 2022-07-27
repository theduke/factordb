use factordb::{
    prelude::{Id, Value},
    query::expr::BinaryOp,
};

use crate::registry::{Registry, ATTR_ID_LOCAL};

use super::{QueryPlan, ResolvedExpr};

pub trait FalliblePlanOptimizer {
    fn optimize(
        &self,
        registry: &Registry,
        plan: &QueryPlan<Value, ResolvedExpr>,
    ) -> Result<Option<QueryPlan<Value, ResolvedExpr>>, anyhow::Error>;
}

pub trait PlanOptimizer {
    fn optimize(
        &self,
        registry: &Registry,
        plan: &QueryPlan<Value, ResolvedExpr>,
    ) -> Option<QueryPlan<Value, ResolvedExpr>>;
}

impl<O: PlanOptimizer> FalliblePlanOptimizer for O {
    fn optimize(
        &self,
        registry: &Registry,
        plan: &QueryPlan<Value, ResolvedExpr>,
    ) -> Result<Option<QueryPlan<Value, ResolvedExpr>>, anyhow::Error> {
        Ok(self.optimize(registry, plan))
    }
}

pub struct StabilizingPlanOptimizer<O>(O);

impl<O: FalliblePlanOptimizer> FalliblePlanOptimizer for StabilizingPlanOptimizer<O> {
    fn optimize(
        &self,
        registry: &Registry,
        plan: &QueryPlan<Value, ResolvedExpr>,
    ) -> Result<Option<QueryPlan<Value, ResolvedExpr>>, anyhow::Error> {
        let mut resolved = match self.0.optimize(registry, plan) {
            Ok(Some(p)) => p,
            other => {
                return other;
            }
        };

        while let Ok(Some(new)) = self.0.optimize(registry, &resolved) {
            resolved = new;
        }

        Ok(Some(resolved))
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

pub struct OptimizeEntitySelect;

impl PlanOptimizer for OptimizeEntitySelect {
    fn optimize(
        &self,
        _reg: &Registry,
        plan: &QueryPlan<Value, ResolvedExpr>,
    ) -> Option<QueryPlan<Value, ResolvedExpr>> {
        plan.map_recurse(|plan| match plan {
            QueryPlan::Scan { filter } => {
                let id = filter.as_ref().and_then(expr_is_entity_id_eq)?;
                Some(QueryPlan::SelectEntity { id })
            }
            // TODO: handle higher level filter also?
            // QueryPlan::Filter { expr, input } => todo!(),
            _ => None,
        })
    }
}

// fn extract_expr<P: Fn(&ResolvedExpr) -> bool>(
//     expr: &ResolvedExpr,
//     predicate: P,
// ) -> Option<(ResolvedExpr, Option<AndOrExpr>)> {
//     match expr {
//         e if predicate(e) => Some((expr.clone(), None)),
//         ResolvedExpr::BinaryOp(bin) => {
//             let op = AndOr::try_from_op(bin.op.clone())?;

//             if predicate(&bin.left) {
//                 Some((
//                     bin.left.clone(),
//                     Some(AndOrExpr {
//                         op,
//                         expr: bin.right.clone(),
//                     }),
//                 ))
//             } else if predicate(&bin.right) {
//                 Some((
//                     bin.right.clone(),
//                     Some(AndOrExpr {
//                         op,
//                         expr: bin.left.clone(),
//                     }),
//                 ))
//             } else {
//                 // TODO: allow further nesting of and/or/not
//                 None
//             }
//         }
//         // TODO: handle not ( x AND/OR y)
//         _ => None,
//     }
// }

/// Extract a partial expression from a possibly nested AND expression.
///
/// returns the matched (extracted) expression, and the remaining AND clause if present.
fn extract_expr_and<P>(
    expr: &ResolvedExpr,
    predicate: P,
) -> Option<(ResolvedExpr, Option<ResolvedExpr>)>
where
    P: Fn(&ResolvedExpr) -> bool + Copy,
{
    if predicate(expr) {
        return Some((expr.clone(), None));
    }

    let (left, right) = expr.as_binary_op_and()?;

    if predicate(left) {
        Some((left.clone(), Some(right.clone())))
    } else if predicate(right) {
        Some((right.clone(), Some(left.clone())))
    } else if let Some((matched, rest)) = extract_expr_and(left, predicate) {
        let remainder = if let Some(rest) = rest {
            ResolvedExpr::and(rest, right.clone())
        } else {
            right.clone()
        };

        Some((matched, Some(remainder)))
    } else if let Some((matched, rest)) = extract_expr_and(right, predicate) {
        let remainder = if let Some(rest) = rest {
            ResolvedExpr::and(left.clone(), rest)
        } else {
            left.clone()
        };

        Some((matched, Some(remainder)))
    } else {
        None
    }
}

fn expr_is_index_select_literal(expr: &ResolvedExpr) -> bool {
    match expr {
        _ if expr.as_binary_op_attr_eq_value().is_some() => true,
        ResolvedExpr::InLiteral { value, items: _ } if value.as_attr().is_some() => true,
        _ => false,
    }
}

pub struct FilterWithIndex;

impl FilterWithIndex {
    fn optimize_inner(
        reg: &Registry,
        plan: &QueryPlan<Value, ResolvedExpr>,
    ) -> Option<QueryPlan<Value, ResolvedExpr>> {
        match plan {
            // QueryPlan::EmptyRelation => todo!(),
            // QueryPlan::SelectEntity { id } => todo!(),
            QueryPlan::Scan { filter } => {
                let filter = filter.as_ref()?;

                let (index_filter, rest) = extract_expr_and(filter, expr_is_index_select_literal)?;

                let (attr, values) =
                    if let Some((attr, value)) = index_filter.as_binary_op_attr_eq_value() {
                        (attr, vec![value.clone()])
                    } else if let Some((attr, values)) = index_filter.as_in_literal_attr() {
                        (attr, values.iter().cloned().collect())
                    } else {
                        // Should never happen...
                        return None;
                    };

                let indexes = reg.indexes_for_attribute(attr);
                if indexes.len() != 1 {
                    return None;
                }
                let index = indexes[0].local_id;

                let mut iter = values.into_iter();

                let plan = QueryPlan::IndexSelect {
                    index,
                    value: iter.next()?,
                };

                let plan = iter.fold(plan, |plan, value| -> QueryPlan<Value, ResolvedExpr> {
                    QueryPlan::Merge {
                        left: Box::new(plan),
                        right: Box::new(QueryPlan::IndexSelect { index, value }),
                    }
                });

                let final_plan = if let Some(rest) = rest {
                    QueryPlan::Filter {
                        expr: rest,
                        input: Box::new(plan),
                    }
                } else {
                    plan
                };

                Some(final_plan)
            }
            _ => None,
            // QueryPlan::Filter { expr, input } => todo!(),
            // QueryPlan::Limit { limit, input } => todo!(),
            // QueryPlan::Skip { count, input } => todo!(),
            // QueryPlan::Merge { left, right } => todo!(),
            // QueryPlan::IndexSelect { index, value } => todo!(),
            // QueryPlan::IndexScan { index, from, until, direction } => todo!(),
            // QueryPlan::IndexScanPrefix { index, direction, prefix } => todo!(),
            // QueryPlan::Sort { sorts, input } => todo!(),
        }
    }
}

impl PlanOptimizer for FilterWithIndex {
    fn optimize(
        &self,
        reg: &Registry,
        plan: &QueryPlan<Value, ResolvedExpr>,
    ) -> Option<QueryPlan<Value, ResolvedExpr>> {
        plan.map_recurse(move |q| Self::optimize_inner(reg, q))
    }
}

#[cfg(test)]
mod tests {
    use factordb::prelude::{AttrType, AttributeDescriptor, Expr, Select};

    use crate::registry::ATTR_TYPE_LOCAL;

    use super::*;

    #[test]
    fn test_optimize_query_use_index_attr_eq() {
        let reg = Registry::new();
        let select = Select::new().with_filter(Expr::eq(AttrType::expr(), "sometype"));
        let plan = super::super::plan_select(select, &reg).unwrap();

        let indexes = reg.indexes_for_attribute(ATTR_TYPE_LOCAL);
        assert_eq!(indexes.len(), 1);
        let index = &indexes[0];

        let expected = QueryPlan::IndexSelect {
            index: index.local_id,
            value: Value::from("sometype"),
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_optimize_query_use_index_attr_eq_with_limit() {
        let reg = Registry::new();
        let select = Select::new()
            .with_filter(Expr::eq(AttrType::expr(), "sometype"))
            .with_limit(10);
        let plan = super::super::plan_select(select, &reg).unwrap();

        let indexes = reg.indexes_for_attribute(ATTR_TYPE_LOCAL);
        assert_eq!(indexes.len(), 1);
        let index = &indexes[0];

        let expected = QueryPlan::Limit {
            limit: 10,
            input: Box::new(QueryPlan::IndexSelect {
                index: index.local_id,
                value: Value::from("sometype"),
            }),
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_optimize_query_use_index_with_extra_and() {
        let reg = Registry::new();
        let select = Select::new()
            .with_filter(Expr::eq(AttrType::expr(), "sometype").and_with(Expr::eq(1, 1)));
        let plan = super::super::plan_select(select, &reg).unwrap();

        let indexes = reg.indexes_for_attribute(ATTR_TYPE_LOCAL);
        assert_eq!(indexes.len(), 1);
        let index = &indexes[0];

        let expected = QueryPlan::Filter {
            expr: ResolvedExpr::BinaryOp(Box::new(super::super::BinaryExpr {
                left: ResolvedExpr::Literal(Value::from(1)),
                op: BinaryOp::Eq,
                right: ResolvedExpr::Literal(Value::from(1)),
            })),
            input: Box::new(QueryPlan::IndexSelect {
                index: index.local_id,
                value: Value::from("sometype"),
            }),
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_optimize_query_use_index_with_extra_nested_and() {
        let reg = Registry::new();
        let select = Select::new().with_filter(Expr::and(
            Expr::eq(1, 1),
            Expr::and(Expr::eq(1, 2), Expr::eq(AttrType::expr(), "sometype")),
        ));
        let plan = super::super::plan_select(select, &reg).unwrap();

        let indexes = reg.indexes_for_attribute(ATTR_TYPE_LOCAL);
        assert_eq!(indexes.len(), 1);
        let index = &indexes[0];

        let expected = QueryPlan::Filter {
            expr: ResolvedExpr::and(
                ResolvedExpr::eq(ResolvedExpr::literal(1), ResolvedExpr::literal(1)),
                ResolvedExpr::eq(ResolvedExpr::literal(1), ResolvedExpr::literal(2)),
            ),
            input: Box::new(QueryPlan::IndexSelect {
                index: index.local_id,
                value: Value::from("sometype"),
            }),
        };

        assert_eq!(plan, expected);
    }
}
