use crate::{
    query::{
        expr::{BinaryOp, Expr, UnaryOp},
        select::{Order, Select},
    },
    registry::{LocalAttributeId, LocalIndexId, Registry, ATTR_ID_LOCAL},
    AnyError, Id, Ident, Value,
};

#[derive(Debug)]
pub struct Sort<E> {
    pub on: E,
    pub order: Order,
}

#[derive(Debug)]
pub enum QueryOp<V = Value, E = Expr> {
    SelectEntity { id: Id },
    Scan,
    Filter { expr: E },
    Limit { limit: u64 },
    Merge { left: Box<Self>, right: Box<Self> },
    IndexSelect { index: LocalIndexId, value: V },
    IndexScan { from: V, until: V },
    IndexScanPrefix { prefix: V },
    Sort { sorts: Vec<Sort<E>> },
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
    /// Select the value of an attribute.
    Attr(LocalAttributeId),
    /// Resolve the value of an [`Ident`] into an [`Id`].
    Ident(Ident),
    UnaryOp {
        op: UnaryOp,
        expr: Box<Self>,
    },
    BinaryOp(Box<BinaryExpr<V>>),
    If {
        value: Box<Self>,
        then: Box<Self>,
        or: Box<Self>,
    },
    Op(Box<QueryOp<V, ResolvedExpr<V>>>),
}

pub fn plan_select(
    query: Select,
    reg: &Registry,
) -> Result<Vec<QueryOp<Value, ResolvedExpr>>, AnyError> {
    let mut ops: Vec<QueryOp<Value, ResolvedExpr>> = vec![];

    if let Some(filter) = query.filter {
        let resolved = resolve_expr(filter, reg)?;
        let optimized = build_select_expr(resolved);

        match optimized {
            ResolvedExpr::Op(op) => ops.push(*op),
            other => {
                ops.push(QueryOp::Scan);
                ops.push(QueryOp::Filter { expr: other });
            }
        }
    } else {
        ops.push(QueryOp::Scan);
    }

    if !query.sort.is_empty() {
        ops.push(QueryOp::Sort {
            sorts: query
                .sort
                .into_iter()
                .map(|s| {
                    Ok(Sort {
                        on: resolve_expr(s.on, reg)?,
                        order: s.order,
                    })
                })
                .collect::<Result<_, anyhow::Error>>()?,
        })
    }
    ops.push(QueryOp::Limit { limit: query.limit });

    Ok(ops)
}

pub fn resolve_expr(expr: Expr, reg: &Registry) -> Result<ResolvedExpr, AnyError> {
    match expr {
        Expr::Literal(v) => Ok(ResolvedExpr::Literal(v)),
        Expr::Attr(ident) => Ok(ResolvedExpr::Attr(
            reg.require_attr_by_ident(&ident)?.local_id,
        )),
        Expr::Ident(ident) => Ok(ResolvedExpr::Ident(ident)),
        Expr::Variable(_v) => Err(anyhow::anyhow!("Query variables not implemented yet")),
        Expr::UnaryOp { op, expr } => Ok(ResolvedExpr::UnaryOp {
            op,
            expr: Box::new(resolve_expr(*expr, reg)?),
        }),
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
