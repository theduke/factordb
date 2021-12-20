use std::{collections::HashMap, convert::TryInto};

use anyhow::{bail, Context};

use crate::{AnyError, Value};

use super::{
    expr::{BinaryOp, Expr},
    select::{Order, Select, Sort},
};
use sqlparser::ast::{Expr as SqlExpr, Value as SqlValue};

pub fn parse_select(sql: &str) -> Result<Select, anyhow::Error> {
    use sqlparser::ast::Statement;

    let statements =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, sql)?;
    let stmt = if statements.is_empty() {
        bail!("No statement detected");
    } else if statements.len() > 1 {
        bail!("Only a single statement is allowed");
    } else {
        statements.into_iter().next().unwrap()
    };

    let query = if let Statement::Query(q) = stmt {
        q
    } else {
        bail!("Expected a `SELECT` statement")
    };

    let select = match query.body {
        sqlparser::ast::SetExpr::Select(s) => s,
        sqlparser::ast::SetExpr::Query(_) => {
            bail!("subqueries are not supported");
        }
        sqlparser::ast::SetExpr::SetOperation { .. } => {
            bail!("set operations (union/intersect/...) are not supported");
        }
        sqlparser::ast::SetExpr::Values(_) => {
            bail!("literal value select not supported");
        }
        sqlparser::ast::SetExpr::Insert(_) => {
            bail!("INSERT not supported");
        }
    };

    if select.distinct {
        bail!("DISTINCT is not supported");
    }
    if select.top.is_some() {
        bail!("MysQL TOP syntax is not supported");
    }
    if !select.lateral_views.is_empty() {
        bail!("LATERAL VIEWS is not supported");
    }

    let filter = select
        .selection
        .map(build_expr)
        .transpose()
        .context("Invalid WHERE clause")?;

    if !select.group_by.is_empty() {
        bail!("GROUP BY is not supported");
    }
    if !select.cluster_by.is_empty() {
        bail!("CLUSTER BY is not supported");
    }
    if !select.distribute_by.is_empty() {
        bail!("DISTRIBUTE BY is not supported");
    }
    if select.having.is_some() {
        bail!("HAVING is not supported");
    }
    if !select.sort_by.is_empty() {
        bail!("SORT BY is not supported");
    }

    if select.projection.len() != 1 || select.projection[0] != sqlparser::ast::SelectItem::Wildcard
    {
        bail!("only wildcard selects are supported (SELECT * FROM ...)");
    }

    if select.from.len() != 1 {
        bail!("must select from the entities table: SELECT * FROM entities");
    }
    let table_with_joins = select.from[0].clone();
    match table_with_joins.relation {
        sqlparser::ast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => {
            if name.0.len() != 1 || name.0[0].value != "entities" {
                bail!("must select from the entities table: SELECT * FROM entities");
            }
            if alias.is_some() {
                bail!("must select from the entities table: SELECT * FROM entities");
            }
            if !args.is_empty() {
                bail!("must select from the entities table: SELECT * FROM entities");
            }
            if !with_hints.is_empty() {
                bail!("must select from the entities table: SELECT * FROM entities");
            }
        }
        _other => {
            bail!("must select from the entities table: SELECT * FROM entities");
        }
    };

    if !table_with_joins.joins.is_empty() {
        bail!("JOINs are not supported");
    }

    let limit = match query.limit {
        Some(SqlExpr::Value(SqlValue::Number(num, _))) => num
            .parse::<u64>()
            .context("Unsupported LIMIT: only constant numbers are supported")?,
        Some(_other) => {
            bail!("Unsupported LIMIT: only constant numbers are supported")
        }
        None => 0,
    };
    let offset = match query.offset.map(|o| o.value) {
        Some(SqlExpr::Value(SqlValue::Number(num, _))) => num
            .parse::<u64>()
            .context("Unsupported LIMIT: only constant numbers are supported")?,
        Some(_other) => {
            bail!("Unsupported LIMIT: only constant numbers are supported")
        }
        None => 0,
    };

    let sort = query
        .order_by
        .into_iter()
        .map(|clause| {
            Ok(Sort {
                on: build_expr(clause.expr)?,
                order: if clause.asc.unwrap_or(true) {
                    Order::Asc
                } else {
                    Order::Desc
                },
            })
        })
        .collect::<Result<Vec<_>, AnyError>>()?;

    Ok(Select {
        filter,
        joins: Vec::new(),
        sort,
        variables: HashMap::new(),
        limit,
        offset,
        cursor: None,
    })
}

fn build_expr(expr: SqlExpr) -> Result<Expr, AnyError> {
    let e = match expr {
        SqlExpr::Identifier(ident) => Expr::Attr(ident.value.into()),
        SqlExpr::Wildcard => {
            // TODO: implement.
            bail!("wildcards not supported")
        }
        SqlExpr::QualifiedWildcard(_) => {
            bail!("wildcards not supported")
        }
        SqlExpr::CompoundIdentifier(_) => {
            bail!("compound identifier not implemented yet")
        }
        SqlExpr::IsNull(e) => Expr::is_null(build_expr(*e)?),
        SqlExpr::IsNotNull(e) => Expr::not(Expr::is_null(build_expr(*e)?)),
        SqlExpr::IsDistinctFrom(_, _) => {
            bail!("IS DISTINCT is not supported")
        }
        SqlExpr::IsNotDistinctFrom(_, _) => {
            bail!("IS NOT DISTINCT is not supported")
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let value = build_expr(*expr)?;
            let items = list
                .into_iter()
                .map(build_expr)
                .collect::<Result<Vec<Expr>, _>>()?;

            let expr = Expr::in_(value, Expr::List(items));

            // let e2 = Expr::in_(build_expr(*expr)?, Expr::);
            if negated {
                Expr::not(expr)
            } else {
                expr
            }
        }
        SqlExpr::InSubquery {
            expr: _,
            subquery: _,
            negated: _,
        } => {
            bail!("subqueries are not supported")
        }
        SqlExpr::Between { .. } => {
            bail!("BETWEEN not supported")
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let left = Box::new(build_expr(*left)?);
            let right = Box::new(build_expr(*right)?);

            let op = match op {
                sqlparser::ast::BinaryOperator::Gt => BinaryOp::Gt,
                sqlparser::ast::BinaryOperator::Lt => BinaryOp::Lt,
                sqlparser::ast::BinaryOperator::GtEq => BinaryOp::Gte,
                sqlparser::ast::BinaryOperator::LtEq => BinaryOp::Lte,
                sqlparser::ast::BinaryOperator::Eq => BinaryOp::Eq,
                sqlparser::ast::BinaryOperator::NotEq => BinaryOp::Neq,
                sqlparser::ast::BinaryOperator::And => BinaryOp::And,
                sqlparser::ast::BinaryOperator::Or => BinaryOp::Or,
                other => {
                    bail!("Comparison operator {} not supported", other);
                }
            };

            Expr::BinaryOp { left, op, right }
        }
        SqlExpr::UnaryOp { op, expr } => {
            let expr = build_expr(*expr)?;
            match op {
                sqlparser::ast::UnaryOperator::Not => Expr::not(expr),
                other => {
                    bail!("Unary operator '{}' not supported", other);
                }
            }
        }
        SqlExpr::Cast { .. } => {
            bail!("CAST not supported");
        }
        SqlExpr::TryCast { .. } => {
            bail!("TRY_CAST not supported");
        }
        SqlExpr::Extract { .. } => {
            bail!("EXTRACT not supported");
        }
        SqlExpr::Substring { .. } => {
            bail!("SUBSTRING not supported");
        }
        SqlExpr::Trim { .. } => {
            bail!("TRIM not supported");
        }
        SqlExpr::Collate { .. } => {
            bail!("COLLATE not supported");
        }
        SqlExpr::Nested(e) => build_expr(*e)?,
        SqlExpr::Value(v) => {
            let value = match v {
                SqlValue::Number(num, _) => {
                    if let Ok(v) = num.parse::<i64>() {
                        if v >= 0 {
                            Value::UInt(v.try_into().unwrap())
                        } else {
                            Value::Int(v)
                        }
                    } else if let Ok(v) = num.parse::<f64>() {
                        Value::Float(v.into())
                    } else {
                        bail!("Invalid number: '{}'", num);
                    }
                }
                SqlValue::SingleQuotedString(s) => Value::String(s),
                SqlValue::DoubleQuotedString(s) => Value::String(s),
                SqlValue::NationalStringLiteral(_) => {
                    bail!("national string literal not supported");
                }
                SqlValue::HexStringLiteral(_) => {
                    bail!("hex literals not supported");
                }
                SqlValue::Boolean(v) => Value::Bool(v),
                SqlValue::Interval { .. } => {
                    bail!("INTERVAL not supported");
                }
                SqlValue::Null => Value::Unit,
            };
            Expr::Literal(value)
        }
        SqlExpr::TypedString { .. } => {
            bail!("no types strings are supported");
        }
        SqlExpr::MapAccess { .. } => {
            bail!("map accessors not supported");
        }
        SqlExpr::Function(_) => {
            bail!("functions not supported");
        }
        SqlExpr::Case { .. } => {
            bail!("CASE is not supported");
        }
        SqlExpr::Exists(_) => {
            bail!("EXISTS is not supported");
        }
        SqlExpr::Subquery(_) => {
            bail!("subqueries are not supported");
        }
        SqlExpr::ListAgg(_) => {
            bail!("LISTAGG is not supported");
        }
        SqlExpr::GroupingSets(_) => {
            bail!("GROUPTING SETS are not supported");
        }
        SqlExpr::Cube(_) => {
            bail!("CUBE is not supported");
        }
        SqlExpr::Rollup(_) => {
            bail!("ROLLUP is not supported");
        }
    };

    Ok(e)
}

#[cfg(test)]
mod tests {
    use crate::{
        prelude::AttributeDescriptor,
        schema::builtin::{AttrIdent, AttrTitle, AttrType},
    };

    use super::*;

    #[test]
    fn test_parse_select_sql() {
        assert_eq!(
            parse_select("SELECT * FROM entities").unwrap(),
            Select::new(),
        );

        assert_eq!(
            parse_select("SELECT * FROM entities LIMIT 42").unwrap(),
            Select::new().with_limit(42),
        );

        assert_eq!(
            parse_select("SELECT * FROM entities OFFSET 42").unwrap(),
            Select::new().with_offset(42),
        );

        // Values.

        assert_eq!(
            parse_select("SELECT * FROM entities WHERE 0 = 1").unwrap(),
            Select::new().with_filter(Expr::eq(0u64, 1u64)),
        );

        // WHERE

        assert_eq!(
            parse_select(r#" SELECT * FROM entities WHERE "factor/type" = 'sometype' "#).unwrap(),
            Select::new().with_filter(Expr::eq(Expr::attr::<AttrType>(), "sometype")),
        );

        assert_eq!(
            parse_select(r#" SELECT * FROM entities WHERE "factor/type" IN ('ty1', 'ty2') "#)
                .unwrap(),
            Select::new().with_filter(Expr::in_(
                Expr::attr::<AttrType>(),
                Expr::List(vec![Expr::from("ty1"), Expr::from("ty2")])
            )),
        );

        // ORDER BY

        assert_eq!(
            parse_select(r#" SELECT * FROM entities order by "factor/title" ASC "#).unwrap(),
            Select::new().with_sort(AttrTitle::expr(), Order::Asc),
        );

        assert_eq!(
            parse_select(r#" SELECT * FROM entities order by "factor/title" DESC "#).unwrap(),
            Select::new().with_sort(AttrTitle::expr(), Order::Desc),
        );

        assert_eq!(
            parse_select(
                r#" SELECT * FROM entities order by "factor/title" DESC, "factor/ident" ASC "#
            )
            .unwrap(),
            Select::new()
                .with_sort(AttrTitle::expr(), Order::Desc)
                .with_sort(AttrIdent::expr(), Order::Asc),
        );
    }
}
