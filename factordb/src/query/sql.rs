use std::{collections::HashMap, convert::TryInto};

use anyhow::{bail, Context};

use crate::{data::Value, query::select::Aggregation, AnyError};

use super::{
    expr::{BinaryOp, Expr},
    select::{Order, Select, Sort},
};
use sqlparser::ast::{Expr as SqlExpr, SelectItem, Value as SqlValue};

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

    let select = match *query.body {
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

    dbg!(&select);

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

    let mut aggregate = Vec::<Aggregation>::new();

    if select.projection.len() == 1 && select.projection[0] == sqlparser::ast::SelectItem::Wildcard
    {
        // Select all attributes.
    } else {
        for proj in select.projection {
            let (is_count, alias) = select_item_as_count(&proj);
            if is_count {
                let name = alias.unwrap_or_else(|| "count".to_string());

                aggregate.push(Aggregation {
                    name,
                    op: crate::query::select::AggregationOp::Count,
                });
            } else {
                bail!("Unsupported SQL projection: {proj:?}");
            }
        }
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
            if args.is_some() {
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
        aggregate,
        sort,
        variables: HashMap::new(),
        limit,
        offset,
        cursor: None,
    })
}

/// Check if a projection is "select count(*) [AS XXX]".
/// Returns (true, None) if it is a count with no alias, (true, Some("alias")) if an alias is given,
/// or (false, None) if not a count expression.
fn select_item_as_count(item: &SelectItem) -> (bool, Option<String>) {
    match item {
        SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(f)) => {
            if f.name.0.len() == 1 && f.name.0[0].value == "count" && f.args.is_empty() {
                (true, None)
            } else {
                (false, None)
            }
        }
        SelectItem::ExprWithAlias {
            expr: sqlparser::ast::Expr::Function(f),
            alias,
        } => {
            if f.name.0.len() == 1 && f.name.0[0].value == "count" && f.args.is_empty() {
                (true, Some(alias.value.clone()))
            } else {
                (false, None)
            }
        }
        _ => (false, None),
    }
}

fn build_expr(expr: SqlExpr) -> Result<Expr, AnyError> {
    let e = match expr {
        SqlExpr::Identifier(ident) => Expr::Attr(ident.value.into()),
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
        SqlExpr::BinaryOp { left, op, right } => match (*left, *right) {
            (SqlExpr::AnyOp(any), other) | (other, SqlExpr::AnyOp(any)) => {
                dbg!(&any, &other);
                let target = match *any {
                    SqlExpr::Identifier(ident) => Expr::Attr(ident.value.into()),
                    other => {
                        bail!("Unsupported ANY operator {:?}: ANY(_) is only supported with an identifer, like ANY(\"my/attribute\")", other);
                    }
                };

                let value = build_expr(other)?;

                match op {
                    sqlparser::ast::BinaryOperator::Eq => Expr::in_(value, target),
                    _ => bail!(
                        "Unsupported ANY operator {:?}: ANY(_) is only supported with =, like ",
                        op
                    ),
                }
            }
            (left, right) => {
                let left = build_expr(left)?;
                let right = build_expr(right)?;
                let op = match op {
                    sqlparser::ast::BinaryOperator::Gt => BinaryOp::Gt,
                    sqlparser::ast::BinaryOperator::Lt => BinaryOp::Lt,
                    sqlparser::ast::BinaryOperator::GtEq => BinaryOp::Gte,
                    sqlparser::ast::BinaryOperator::LtEq => BinaryOp::Lte,
                    sqlparser::ast::BinaryOperator::Eq => BinaryOp::Eq,
                    sqlparser::ast::BinaryOperator::NotEq => BinaryOp::Neq,
                    sqlparser::ast::BinaryOperator::And => BinaryOp::And,
                    sqlparser::ast::BinaryOperator::Or => BinaryOp::Or,
                    sqlparser::ast::BinaryOperator::Like => BinaryOp::Contains,
                    // sqlparser::ast::BinaryOperator::Plus => todo!(),
                    // sqlparser::ast::BinaryOperator::Minus => todo!(),
                    // sqlparser::ast::BinaryOperator::Multiply => todo!(),
                    // sqlparser::ast::BinaryOperator::Divide => todo!(),
                    // sqlparser::ast::BinaryOperator::Modulo => todo!(),
                    // sqlparser::ast::BinaryOperator::StringConcat => todo!(),
                    // sqlparser::ast::BinaryOperator::Spaceship => todo!(),
                    // sqlparser::ast::BinaryOperator::Xor => todo!(),
                    // sqlparser::ast::BinaryOperator::NotLike => todo!(),
                    // sqlparser::ast::BinaryOperator::ILike => todo!(),
                    // sqlparser::ast::BinaryOperator::NotILike => todo!(),
                    // sqlparser::ast::BinaryOperator::BitwiseOr => todo!(),
                    // sqlparser::ast::BinaryOperator::BitwiseAnd => todo!(),
                    // sqlparser::ast::BinaryOperator::BitwiseXor => todo!(),
                    // sqlparser::ast::BinaryOperator::PGBitwiseXor => todo!(),
                    // sqlparser::ast::BinaryOperator::PGBitwiseShiftLeft => todo!(),
                    // sqlparser::ast::BinaryOperator::PGBitwiseShiftRight => todo!(),
                    // sqlparser::ast::BinaryOperator::PGRegexNotMatch => todo!(),
                    // sqlparser::ast::BinaryOperator::PGRegexNotIMatch => todo!(),
                    sqlparser::ast::BinaryOperator::PGRegexMatch => BinaryOp::RegexMatch,
                    sqlparser::ast::BinaryOperator::PGRegexIMatch => {
                        BinaryOp::RegexMatchCaseInsensitive
                    }
                    other => {
                        bail!("Comparison operator {} not supported", other);
                    }
                };

                Expr::binary(left, op, right)
            }
        },
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
                SqlValue::Placeholder(_) => {
                    bail!("Placeholder is not supported");
                }
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
                SqlValue::EscapedStringLiteral(_) => {
                    bail!("escaped string literal not supported");
                }
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
        SqlExpr::Exists { .. } => {
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
        SqlExpr::Tuple(_) => {
            bail!("ROW/TUPLE is not supported");
        }
        SqlExpr::ArrayIndex { .. } => {
            bail!("Array Index is not supported");
        }
        SqlExpr::Array(_) => {
            bail!("Array Index is not supported");
        }
        SqlExpr::InUnnest { .. } => {
            bail!("UNNEST is not supported");
        }
        SqlExpr::JsonAccess { .. } => {
            bail!("json accessors not supported");
        }
        SqlExpr::CompositeAccess { .. } => {
            bail!("composite accessors not supported");
        }
        SqlExpr::IsFalse(_) => {
            bail!("IS FALSE is not supported");
        }
        SqlExpr::IsTrue(_) => {
            bail!("IS TRUE is not supported");
        }
        SqlExpr::AnyOp(_) => {
            bail!("ANY(x) is not supported");
        }
        SqlExpr::AllOp(_) => {
            bail!("ALL(x) is not supported");
        }
        SqlExpr::Position { .. } => {
            bail!("POSITION is not supported");
        }
        SqlExpr::AtTimeZone { .. } => {
            bail!("AT TIME ZONE is not supported");
        }
    };

    Ok(e)
}

#[cfg(test)]
mod tests {
    use crate::{
        prelude::AttributeMeta,
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

        assert_eq!(
            parse_select(r#" SELECT count() FROM entities"#).unwrap(),
            Select::new().with_aggregate(
                crate::query::select::AggregationOp::Count,
                "count".to_string()
            )
        );

        assert_eq!(
            parse_select(r#" SELECT * from entities where 'hello' = ANY("my/attr")  "#).unwrap(),
            Select::new().with_filter(Expr::in_("hello", Expr::attr_ident("my/attr"),))
        );

        assert_eq!(
            parse_select(r#" SELECT * from entities where "a" ~ 'hello' "#).unwrap(),
            Select::new().with_filter(Expr::regex_match(Expr::attr_ident("a"), "hello"))
        );

        assert_eq!(
            parse_select(r#" SELECT * from entities where "a" ~* 'hello' "#).unwrap(),
            Select::new().with_filter(Expr::regex_match_case_insensitive(
                Expr::attr_ident("a"),
                "hello"
            ))
        );
    }
}
