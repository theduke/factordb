use std::time::{Duration, Instant};

use factordb::{prelude::Value, query::expr::BinaryOp};

use super::{BinaryExpr, ResolvedExpr};

pub trait OwnedExprOptimizer {
    fn optimize(&self, expr: ResolvedExpr) -> ResolvedExpr;
}

pub trait ExprOptimizer {
    fn optimize(&self, expr: &ResolvedExpr) -> Option<ResolvedExpr>;
}

pub struct StabilizingExprOptimizer<E>(E);

impl<E: ExprOptimizer> ExprOptimizer for StabilizingExprOptimizer<E> {
    fn optimize(&self, expr: &ResolvedExpr) -> Option<ResolvedExpr> {
        let mut resolved = self.0.optimize(expr)?;

        while let Some(new) = self.0.optimize(&resolved) {
            resolved = new;
        }
        Some(resolved)
    }
}

pub struct LimitedStabilizingExprOptimizer<E> {
    inner: E,
    max_attempts: u32,
    max_duration: Option<Duration>,
}

impl<E: ExprOptimizer> ExprOptimizer for LimitedStabilizingExprOptimizer<E> {
    fn optimize(&self, expr: &ResolvedExpr) -> Option<ResolvedExpr> {
        let max_attempts = self.max_attempts;
        let mut attempts = 0;

        if let Some(duration) = self.max_duration {
            let start = Instant::now();

            let mut resolved = self.inner.optimize(expr)?;
            attempts += 1;

            while attempts < max_attempts && start.elapsed() < duration {
                if let Some(new) = self.optimize(&resolved) {
                    resolved = new;
                    attempts += 1;
                }
            }
            Some(resolved)
        } else {
            let mut resolved = self.inner.optimize(expr)?;
            attempts += 1;

            while attempts < max_attempts {
                if let Some(new) = self.optimize(&resolved) {
                    resolved = new;
                    attempts += 1;
                } else {
                    return Some(resolved);
                }
            }

            Some(resolved)
        }
    }
}

// fn expr_map_once_recurse<F: Fn(&ResolvedExpr) -> Option<ResolvedExpr>>(
//     expr: &ResolvedExpr,
//     mapper: F,
// ) -> Option<ResolvedExpr> {
//     if let Some(e) = mapper(expr) {
//         return Some(e);
//     }
//     match expr {
//         ResolvedExpr::Literal(_) => None,
//         ResolvedExpr::Regex(_) => None,
//         ResolvedExpr::List(list) => {
//             let mut new_list = Vec::new();
//             let mut any_new = false;
//             for item in list {
//                 let x = if let Some(new) = mapper(item) {
//                     any_new = true;
//                     new
//                 } else {
//                     item.clone()
//                 };
//                 new_list.push(x);
//             }

//             if any_new {
//                 Some(ResolvedExpr::List(new_list))
//             } else {
//                 None
//             }
//         }
//         ResolvedExpr::Attr(_) => None,
//         ResolvedExpr::Ident(_) => None,
//         ResolvedExpr::UnaryOp { op, expr } => {
//             let new = mapper(&expr)?;
//             Some(ResolvedExpr::UnaryOp {
//                 op: op.clone(),
//                 expr: Box::new(new),
//             })
//         }
//         ResolvedExpr::BinaryOp(bin) => {
//             if let Some(new) = mapper(&bin.left) {
//                 Some(ResolvedExpr::BinaryOp(Box::new(BinaryExpr {
//                     left: new,
//                     op: bin.op.clone(),
//                     right: bin.right.clone(),
//                 })))
//             } else if let Some(new) = mapper(&bin.right) {
//                 Some(ResolvedExpr::BinaryOp(Box::new(BinaryExpr {
//                     left: bin.left.clone(),
//                     op: bin.op.clone(),
//                     right: new,
//                 })))
//             } else {
//                 None
//             }
//         }
//         ResolvedExpr::InLiteral { value, items: _ } => {
//             if let Some(new) = mapper(&value) {
//                 if let Some(x) = mapper(&new) {
//                     Some(x)
//                 } else {
//                     Some(new)
//                 }
//             } else {
//                 mapper(expr)
//             }
//         }
//         ResolvedExpr::If { .. } => {
//             todo!()
//         }
//     }
// }

fn expr_map_all_recurse<F, V>(expr: ResolvedExpr<V>, mapper: F) -> ResolvedExpr<V>
where
    F: Fn(ResolvedExpr<V>) -> ResolvedExpr<V> + Copy,
    V: std::hash::Hash,
{
    match expr {
        ResolvedExpr::Literal(_)
        | ResolvedExpr::Regex(_)
        | ResolvedExpr::Attr(_)
        | ResolvedExpr::Ident(_) => mapper(expr),
        ResolvedExpr::List(list) => {
            let new_list = list
                .into_iter()
                .map(|e| expr_map_all_recurse(e, mapper))
                .collect();
            let new = ResolvedExpr::List(new_list);
            mapper(new)
        }
        ResolvedExpr::UnaryOp { op, expr } => {
            let new = ResolvedExpr::UnaryOp {
                op,
                expr: Box::new(expr_map_all_recurse(*expr, mapper)),
            };
            mapper(new)
        }
        ResolvedExpr::BinaryOp(bin) => {
            let left = expr_map_all_recurse(bin.left, mapper);
            let right = expr_map_all_recurse(bin.right, mapper);
            let new = ResolvedExpr::BinaryOp(Box::new(BinaryExpr {
                left,
                op: bin.op,
                right,
            }));
            mapper(new)
        }
        ResolvedExpr::InLiteral { value, items } => {
            let new_value = expr_map_all_recurse(*value, mapper);
            let new = ResolvedExpr::InLiteral {
                value: Box::new(new_value),
                items,
            };
            mapper(new)
        }
        ResolvedExpr::If { value, then, or } => {
            let new = ResolvedExpr::If {
                value: Box::new(expr_map_all_recurse(*value, mapper)),
                then: Box::new(expr_map_all_recurse(*then, mapper)),
                or: Box::new(expr_map_all_recurse(*or, mapper)),
            };

            mapper(new)
        }
    }
}

pub struct BinaryToInLiteral;

impl OwnedExprOptimizer for BinaryToInLiteral {
    fn optimize(&self, expr: ResolvedExpr) -> ResolvedExpr {
        expr_map_all_recurse(expr, |expr| match expr {
            ResolvedExpr::BinaryOp(bin) if bin.op == BinaryOp::In => {
                if let ResolvedExpr::Literal(Value::List(values)) = bin.right {
                    ResolvedExpr::InLiteral {
                        value: Box::new(bin.left),
                        items: values.into_iter().collect(),
                    }
                } else {
                    ResolvedExpr::BinaryOp(bin)
                }
            }
            _ => expr,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use factordb::prelude::{AttrType, AttributeMeta, Expr};

    use crate::registry::{Registry, ATTR_TYPE_LOCAL};

    use super::*;

    #[test]
    fn test_expr_optimize_binary_to_in_literal() {
        let reg = Registry::new();
        let expr = Expr::in_(
            AttrType::expr(),
            Expr::Literal(Value::List(vec!["hello".into()])),
        );
        let built = super::super::resolve_expr(expr, &reg).unwrap();
        let opt = super::super::optimize_expr(built);

        let expected = ResolvedExpr::InLiteral {
            value: Box::new(ResolvedExpr::Attr(ATTR_TYPE_LOCAL)),
            items: HashSet::from([Value::from("hello")]),
        };
        assert_eq!(opt, expected);
    }
}
