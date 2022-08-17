use std::{collections::HashMap, convert::TryInto};

use crate::{data::Value, query::select::Aggregation};

use super::{
    expr::{BinaryOp, Expr},
    select::{Order, Select, Sort},
};
use sqlparser::ast::{Expr as SqlExpr, SelectItem, Value as SqlValue};

#[derive(Debug)]
pub struct SqlParseError {
    message: String,
    cause: Option<sqlparser::parser::ParserError>,
}

impl SqlParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            cause: None,
        }
    }
}

impl std::fmt::Display for SqlParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not parse SQL query: {}", self.message)?;
        if let Some(cause) = &self.cause {
            write!(f, ": {}", cause)?;
        }
        Ok(())
    }
}

impl std::error::Error for SqlParseError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        if let Some(err) = &self.cause {
            Some(err)
        } else {
            None
        }
    }
}

pub fn parse_select(sql: &str) -> Result<Select, SqlParseError> {
    use sqlparser::ast::Statement;

    let statements =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, sql).map_err(
            |err| SqlParseError {
                message: "invalid SQL".to_string(),
                cause: Some(err),
            },
        )?;
    let stmt = if statements.is_empty() {
        return Err(SqlParseError::new("No statement detected"));
    } else if statements.len() > 1 {
        return Err(SqlParseError::new("Only a single statement is allowed"));
    } else {
        statements.into_iter().next().unwrap()
    };

    let query = if let Statement::Query(q) = stmt {
        q
    } else {
        return Err(SqlParseError::new("Expected a `SELECT` statement"));
    };

    let select = match *query.body {
        sqlparser::ast::SetExpr::Select(s) => s,
        sqlparser::ast::SetExpr::Query(_) => {
            return Err(SqlParseError::new("subqueries are not supported"));
        }
        sqlparser::ast::SetExpr::SetOperation { .. } => {
            return Err(SqlParseError::new(
                "set operations (union/intersect/...) are not supported",
            ));
        }
        sqlparser::ast::SetExpr::Values(_) => {
            return Err(SqlParseError::new("literal value select not supported"));
        }
        sqlparser::ast::SetExpr::Insert(_) => {
            return Err(SqlParseError::new("INSERT not supported"));
        }
    };

    if select.distinct {
        return Err(SqlParseError::new("DISTINCT is not supported"));
    }
    if select.top.is_some() {
        return Err(SqlParseError::new("MysQL TOP syntax is not supported"));
    }
    if !select.lateral_views.is_empty() {
        return Err(SqlParseError::new("LATERAL VIEWS is not supported"));
    }

    let filter = select
        .selection
        .map(build_expr)
        .transpose()
        .map_err(|mut err| {
            err.message = format!("Invalid WHERE clause: {}", err.message);
            err
        })?;

    if !select.group_by.is_empty() {
        return Err(SqlParseError::new("GROUP BY is not supported"));
    }
    if !select.cluster_by.is_empty() {
        return Err(SqlParseError::new("CLUSTER BY is not supported"));
    }
    if !select.distribute_by.is_empty() {
        return Err(SqlParseError::new("DISTRIBUTE BY is not supported"));
    }
    if select.having.is_some() {
        return Err(SqlParseError::new("HAVING is not supported"));
    }
    if !select.sort_by.is_empty() {
        return Err(SqlParseError::new("SORT BY is not supported"));
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
                return Err(SqlParseError::new("Unsupported SQL projection: {proj:?}"));
            }
        }
    }

    if select.from.len() != 1 {
        return Err(SqlParseError::new(
            "must select from the entities/e table: SELECT * FROM entities / SELECT * FROM e",
        ));
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
                return Err(SqlParseError::new(
                    "must select from the entities table: SELECT * FROM entities",
                ));
            }
            if alias.is_some() {
                return Err(SqlParseError::new(
                    "must select from the entities table: SELECT * FROM entities",
                ));
            }
            if args.is_some() {
                return Err(SqlParseError::new(
                    "must select from the entities table: SELECT * FROM entities",
                ));
            }
            if !with_hints.is_empty() {
                return Err(SqlParseError::new(
                    "must select from the entities table: SELECT * FROM entities",
                ));
            }
        }
        _other => {
            return Err(SqlParseError::new(
                "must select from the entities table: SELECT * FROM entities",
            ));
        }
    };

    if !table_with_joins.joins.is_empty() {
        return Err(SqlParseError::new("JOINs are not supported"));
    }

    let limit = match query.limit {
        Some(SqlExpr::Value(SqlValue::Number(num, _))) => num.parse::<u64>().map_err(|_| {
            SqlParseError::new("Unsupported LIMIT: only constant numbers are supported")
        })?,
        Some(_other) => {
            return Err(SqlParseError::new(
                "Unsupported LIMIT: only constant numbers are supported",
            ))
        }
        None => 0,
    };
    let offset = match query.offset.map(|o| o.value) {
        Some(SqlExpr::Value(SqlValue::Number(num, _))) => num.parse::<u64>().map_err(|_err| {
            SqlParseError::new("Unsupported LIMIT: number must be a positive integer")
        })?,
        Some(_other) => {
            return Err(SqlParseError::new(
                "Unsupported LIMIT: only constant numbers are supported",
            ))
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
        .collect::<Result<Vec<_>, SqlParseError>>()?;

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

fn build_expr(expr: SqlExpr) -> Result<Expr, SqlParseError> {
    let e = match expr {
        SqlExpr::Identifier(ident) => Expr::Attr(ident.value.into()),
        SqlExpr::CompoundIdentifier(_) => {
            return Err(SqlParseError::new(
                "compound identifier not implemented yet",
            ));
        }
        SqlExpr::IsNull(e) => Expr::is_null(build_expr(*e)?),
        SqlExpr::IsNotNull(e) => Expr::not(Expr::is_null(build_expr(*e)?)),
        SqlExpr::IsDistinctFrom(_, _) => {
            return Err(SqlParseError::new("IS DISTINCT is not supported"));
        }
        SqlExpr::IsNotDistinctFrom(_, _) => {
            return Err(SqlParseError::new("IS NOT DISTINCT is not supported"));
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
            return Err(SqlParseError::new("subqueries are not supported"));
        }
        SqlExpr::Between { .. } => {
            return Err(SqlParseError::new("BETWEEN not supported"));
        }
        SqlExpr::BinaryOp { left, op, right } => match (*left, *right) {
            (SqlExpr::AnyOp(any), other) | (other, SqlExpr::AnyOp(any)) => {
                dbg!(&any, &other);
                let target = match *any {
                    SqlExpr::Identifier(ident) => Expr::Attr(ident.value.into()),
                    other => {
                        return Err(SqlParseError::new(format!("Unsupported ANY operator {:?}: ANY(_) is only supported with an identifer, like ANY(\"my/attribute\")", other)));
                    }
                };

                let value = build_expr(other)?;

                match op {
                    sqlparser::ast::BinaryOperator::Eq => Expr::in_(value, target),
                    _ => {
                        return Err(SqlParseError::new(format!(
                            "Unsupported ANY operator {:?}: ANY(_) is only supported with =, like ",
                            op
                        )));
                    }
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
                        return Err(SqlParseError::new(format!(
                            "Comparison operator {} not supported",
                            other
                        )));
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
                    return Err(SqlParseError::new(format!(
                        "Unary operator '{}' not supported",
                        other
                    )));
                }
            }
        }
        SqlExpr::Cast { .. } => {
            return Err(SqlParseError::new("CAST not supported"));
        }
        SqlExpr::TryCast { .. } => {
            return Err(SqlParseError::new("TRY_CAST not supported"));
        }
        SqlExpr::Extract { .. } => {
            return Err(SqlParseError::new("EXTRACT not supported"));
        }
        SqlExpr::Substring { .. } => {
            return Err(SqlParseError::new("SUBSTRING not supported"));
        }
        SqlExpr::Trim { .. } => {
            return Err(SqlParseError::new("TRIM not supported"));
        }
        SqlExpr::Collate { .. } => {
            return Err(SqlParseError::new("COLLATE not supported"));
        }
        SqlExpr::Nested(e) => build_expr(*e)?,
        SqlExpr::Value(v) => {
            let value = match v {
                SqlValue::Placeholder(_) => {
                    return Err(SqlParseError::new("Placeholder is not supported"));
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
                        return Err(SqlParseError::new(format!("Invalid number: '{}'", num)));
                    }
                }
                SqlValue::SingleQuotedString(s) => Value::String(s),
                SqlValue::DoubleQuotedString(s) => Value::String(s),
                SqlValue::NationalStringLiteral(_) => {
                    return Err(SqlParseError::new("national string literal not supported"));
                }
                SqlValue::HexStringLiteral(_) => {
                    return Err(SqlParseError::new(format!("hex literals not supported")));
                }
                SqlValue::Boolean(v) => Value::Bool(v),
                SqlValue::Interval { .. } => {
                    return Err(SqlParseError::new(format!("INTERVAL not supported")));
                }
                SqlValue::Null => Value::Unit,
                SqlValue::EscapedStringLiteral(_) => {
                    return Err(SqlParseError::new(format!(
                        "escaped string literal not supported"
                    )));
                }
            };
            Expr::Literal(value)
        }
        SqlExpr::TypedString { .. } => {
            return Err(SqlParseError::new(format!(
                "no types strings are supported"
            )));
        }
        SqlExpr::MapAccess { .. } => {
            return Err(SqlParseError::new(format!("map accessors not supported")));
        }
        SqlExpr::Function(_) => {
            return Err(SqlParseError::new(format!("functions not supported")));
        }
        SqlExpr::Case { .. } => {
            return Err(SqlParseError::new(format!("CASE is not supported")));
        }
        SqlExpr::Exists { .. } => {
            return Err(SqlParseError::new(format!("EXISTS is not supported")));
        }
        SqlExpr::Subquery(_) => {
            return Err(SqlParseError::new(format!("subqueries are not supported")));
        }
        SqlExpr::ListAgg(_) => {
            return Err(SqlParseError::new(format!("LISTAGG is not supported")));
        }
        SqlExpr::GroupingSets(_) => {
            return Err(SqlParseError::new(format!(
                "GROUPTING SETS are not supported"
            )));
        }
        SqlExpr::Cube(_) => {
            return Err(SqlParseError::new(format!("CUBE is not supported")));
        }
        SqlExpr::Rollup(_) => {
            return Err(SqlParseError::new(format!("ROLLUP is not supported")));
        }
        SqlExpr::Tuple(_) => {
            return Err(SqlParseError::new(format!("ROW/TUPLE is not supported")));
        }
        SqlExpr::ArrayIndex { .. } => {
            return Err(SqlParseError::new(format!("Array Index is not supported")));
        }
        SqlExpr::Array(_) => {
            return Err(SqlParseError::new(format!("Array Index is not supported")));
        }
        SqlExpr::InUnnest { .. } => {
            return Err(SqlParseError::new(format!("UNNEST is not supported")));
        }
        SqlExpr::JsonAccess { .. } => {
            return Err(SqlParseError::new(format!("json accessors not supported")));
        }
        SqlExpr::CompositeAccess { .. } => {
            return Err(SqlParseError::new(format!(
                "composite accessors not supported"
            )));
        }
        SqlExpr::IsFalse(_) => {
            return Err(SqlParseError::new(format!("IS FALSE is not supported")));
        }
        SqlExpr::IsTrue(_) => {
            return Err(SqlParseError::new(format!("IS TRUE is not supported")));
        }
        SqlExpr::AnyOp(_) => {
            return Err(SqlParseError::new(format!("ANY(x) is not supported")));
        }
        SqlExpr::AllOp(_) => {
            return Err(SqlParseError::new(format!("ALL(x) is not supported")));
        }
        SqlExpr::Position { .. } => {
            return Err(SqlParseError::new(format!("POSITION is not supported")));
        }
        SqlExpr::AtTimeZone { .. } => {
            return Err(SqlParseError::new(format!("AT TIME ZONE is not supported")));
        }
    };

    Ok(e)
}

#[cfg(test)]
mod tests {
    use crate::schema::{
        builtin::{AttrIdent, AttrTitle, AttrType},
        AttributeMeta,
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
