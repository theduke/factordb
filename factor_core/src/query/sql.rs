use std::{collections::HashMap, convert::TryInto};

use crate::{
    data::{
        patch::{Patch, PatchPathElem},
        Value, ValueType,
    },
    query::select::Aggregation,
};

use super::{
    expr::{BinaryOp, Expr},
    mutate::{MutateSelect, MutateSelectAction},
    select::{Order, Select, Sort},
};
use sqlparser::ast::{self, Expr as SqlExpr, SelectItem, TableFactor, Value as SqlValue};

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

#[derive(Clone, Debug)]
pub enum ParsedSqlQuery {
    Select(Select),
    Mutate(MutateSelect),
}

impl ParsedSqlQuery {
    pub fn as_mutate(&self) -> Option<&MutateSelect> {
        if let Self::Mutate(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

/// Contains all data from [`slqparser::ast::Insert`].
/// Wrapper needed for code convenience, since it's a flat enum.
struct SqlDelete {
    table_name: TableFactor,
    using: Option<TableFactor>,
    selection: Option<ast::Expr>,
}

struct SqlUpdate {
    /// TABLE
    table: ast::TableWithJoins,
    /// Column assignments
    assignments: Vec<ast::Assignment>,
    /// Table which provide value to be set
    from: Option<ast::TableWithJoins>,
    /// WHERE
    selection: Option<ast::Expr>,
}

pub fn parse_sql(sql: &str) -> Result<ParsedSqlQuery, SqlParseError> {
    match parse_single_statement(sql)? {
        ast::Statement::Query(q) => build_select(*q).map(ParsedSqlQuery::Select),
        ast::Statement::Update {
            table,
            assignments,
            from,
            selection,
        } => {
            let update = SqlUpdate {
                table,
                assignments,
                from,
                selection,
            };
            build_update(update).map(ParsedSqlQuery::Mutate)
        }
        ast::Statement::Delete {
            table_name,
            using,
            selection,
        } => {
            let del = SqlDelete {
                table_name,
                using,
                selection,
            };
            build_delete(del).map(ParsedSqlQuery::Mutate)
        }
        _other => Err(SqlParseError::new(
            "Unsupported SQL statement: expected SELECT/UPDATE/DELETE",
        )),
    }
}

pub fn parse_select(sql: &str) -> Result<Select, SqlParseError> {
    let query = parse_query(sql)?;
    build_select(query)
}

fn parse_single_statement(sql: &str) -> Result<ast::Statement, SqlParseError> {
    let statements =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, sql).map_err(
            |err| SqlParseError {
                message: "invalid SQL".to_string(),
                cause: Some(err),
            },
        )?;
    if statements.is_empty() {
        Err(SqlParseError::new("No statement detected"))
    } else if statements.len() > 1 {
        Err(SqlParseError::new("Only a single statement is allowed"))
    } else {
        Ok(statements.into_iter().next().unwrap())
    }
}

fn parse_query(sql: &str) -> Result<ast::Query, SqlParseError> {
    let stmt = parse_single_statement(sql)?;
    if let ast::Statement::Query(q) = stmt {
        Ok(*q)
    } else {
        Err(SqlParseError::new("Expected a `SELECT` statement"))
    }
}

fn validate_table_factor(factor: &ast::TableFactor) -> Result<(), SqlParseError> {
    match factor {
        ast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => {
            let table_name = if name.0.len() == 1 {
                name.0[0].value.clone()
            } else {
                return Err(SqlParseError::new(
                    "must select from the entities table: SELECT * FROM entities",
                ));
            };

            match table_name.as_str() {
                "entities" | "e" => {}
                _other => {
                    return Err(SqlParseError::new(
                        "must select from the entities/e table: SELECT * FROM entities / SELECT * FROM e",
                    ));
                }
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
    Ok(())
}

fn validate_table_with_joins(t: &ast::TableWithJoins) -> Result<(), SqlParseError> {
    validate_table_factor(&t.relation)?;
    if !t.joins.is_empty() {
        return Err(SqlParseError::new("JOINs are not supported"));
    }
    Ok(())
}

pub fn build_select(query: ast::Query) -> Result<Select, SqlParseError> {
    let select = match *query.body {
        ast::SetExpr::Select(s) => s,
        ast::SetExpr::Query(_) => {
            return Err(SqlParseError::new("subqueries are not supported"));
        }
        ast::SetExpr::SetOperation { .. } => {
            return Err(SqlParseError::new(
                "set operations (union/intersect/...) are not supported",
            ));
        }
        ast::SetExpr::Values(_) => {
            return Err(SqlParseError::new("literal value select not supported"));
        }
        ast::SetExpr::Insert(_) => {
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

    if select.projection.len() == 1 && select.projection[0] == ast::SelectItem::Wildcard {
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
    validate_table_with_joins(&table_with_joins)?;

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
        SelectItem::UnnamedExpr(ast::Expr::Function(f)) => {
            if f.name.0.len() == 1 && f.name.0[0].value == "count" && f.args.is_empty() {
                (true, None)
            } else {
                (false, None)
            }
        }
        SelectItem::ExprWithAlias {
            expr: ast::Expr::Function(f),
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
                let target = match *any {
                    SqlExpr::Identifier(ident) => Expr::Attr(ident.value.into()),
                    other => {
                        return Err(SqlParseError::new(format!("Unsupported ANY operator {:?}: ANY(_) is only supported with an identifer, like ANY(\"my/attribute\")", other)));
                    }
                };

                let value = build_expr(other)?;

                match op {
                    ast::BinaryOperator::Eq => Expr::in_(value, target),
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
                    ast::BinaryOperator::Gt => BinaryOp::Gt,
                    ast::BinaryOperator::Lt => BinaryOp::Lt,
                    ast::BinaryOperator::GtEq => BinaryOp::Gte,
                    ast::BinaryOperator::LtEq => BinaryOp::Lte,
                    ast::BinaryOperator::Eq => BinaryOp::Eq,
                    ast::BinaryOperator::NotEq => BinaryOp::Neq,
                    ast::BinaryOperator::And => BinaryOp::And,
                    ast::BinaryOperator::Or => BinaryOp::Or,
                    // ast::BinaryOperator::Plus => todo!(),
                    // ast::BinaryOperator::Minus => todo!(),
                    // ast::BinaryOperator::Multiply => todo!(),
                    // ast::BinaryOperator::Divide => todo!(),
                    // ast::BinaryOperator::Modulo => todo!(),
                    // ast::BinaryOperator::StringConcat => todo!(),
                    // ast::BinaryOperator::Spaceship => todo!(),
                    // ast::BinaryOperator::Xor => todo!(),
                    // ast::BinaryOperator::NotLike => todo!(),
                    // ast::BinaryOperator::ILike => todo!(),
                    // ast::BinaryOperator::NotILike => todo!(),
                    // ast::BinaryOperator::BitwiseOr => todo!(),
                    // ast::BinaryOperator::BitwiseAnd => todo!(),
                    // ast::BinaryOperator::BitwiseXor => todo!(),
                    // ast::BinaryOperator::PGBitwiseXor => todo!(),
                    // ast::BinaryOperator::PGBitwiseShiftLeft => todo!(),
                    // ast::BinaryOperator::PGBitwiseShiftRight => todo!(),
                    // ast::BinaryOperator::PGRegexNotMatch => todo!(),
                    // ast::BinaryOperator::PGRegexNotIMatch => todo!(),
                    ast::BinaryOperator::PGRegexMatch => BinaryOp::RegexMatch,
                    ast::BinaryOperator::PGRegexIMatch => BinaryOp::RegexMatchCaseInsensitive,
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
                ast::UnaryOperator::Not => Expr::not(expr),
                other => {
                    return Err(SqlParseError::new(format!(
                        "Unary operator '{}' not supported",
                        other
                    )));
                }
            }
        }
        SqlExpr::Cast { expr, data_type } => {
            use ast::DataType;

            let e = build_expr(*expr)?;
            let mut value = match e {
                Expr::Literal(v) => v,
                _other => {
                    return Err(SqlParseError::new(
                        "Invalid CAST: cast is only supported with literal values",
                    ));
                }
            };

            let res = match data_type {
                DataType::Uuid => value.coerce_mut(&ValueType::Ref),
                DataType::Text
                | DataType::String
                | DataType::Char(_)
                | DataType::Varchar(_)
                | DataType::Nvarchar(_) => value.coerce_mut(&ValueType::String),
                DataType::TinyInt(_)
                | DataType::SmallInt(_)
                | DataType::Int(_)
                | DataType::Integer(_)
                | DataType::BigInt(_) => value.coerce_mut(&ValueType::Int),
                DataType::UnsignedTinyInt(_)
                | DataType::UnsignedSmallInt(_)
                | DataType::UnsignedInt(_)
                | DataType::UnsignedInteger(_)
                | DataType::UnsignedBigInt(_) => value.coerce_mut(&ValueType::UInt),
                DataType::Binary(_) | DataType::Varbinary(_) | DataType::Blob(_) => {
                    value.coerce_mut(&ValueType::Bytes)
                }
                DataType::Float(_) | DataType::Double | DataType::Real => {
                    value.coerce_mut(&ValueType::Float)
                }
                DataType::Boolean => value.coerce_mut(&ValueType::Bool),
                _other => {
                    return Err(SqlParseError::new("Unsupported cast"));
                }
            };

            res.map_err(|err| SqlParseError::new(format!("Unsupported cast: {err}")))?;
            Expr::Literal(value)
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
                    return Err(SqlParseError::new("hex literals not supported".to_string()));
                }
                SqlValue::Boolean(v) => Value::Bool(v),
                SqlValue::Interval { .. } => {
                    return Err(SqlParseError::new("INTERVAL not supported".to_string()));
                }
                SqlValue::Null => Value::Unit,
                SqlValue::EscapedStringLiteral(_) => {
                    return Err(SqlParseError::new(
                        "escaped string literal not supported".to_string(),
                    ));
                }
            };
            Expr::Literal(value)
        }
        SqlExpr::TypedString { .. } => {
            return Err(SqlParseError::new("no types strings are supported"));
        }
        SqlExpr::MapAccess { .. } => {
            return Err(SqlParseError::new("map accessors not supported"));
        }
        SqlExpr::Function(_) => {
            return Err(SqlParseError::new("functions not supported"));
        }
        SqlExpr::Case { .. } => {
            return Err(SqlParseError::new("CASE is not supported"));
        }
        SqlExpr::Exists { .. } => {
            return Err(SqlParseError::new("EXISTS is not supported"));
        }
        SqlExpr::Subquery(_) => {
            return Err(SqlParseError::new("subqueries are not supported"));
        }
        SqlExpr::ListAgg(_) => {
            return Err(SqlParseError::new("LISTAGG is not supported"));
        }
        SqlExpr::GroupingSets(_) => {
            return Err(SqlParseError::new("GROUPTING SETS are not supported"));
        }
        SqlExpr::Cube(_) => {
            return Err(SqlParseError::new("CUBE is not supported"));
        }
        SqlExpr::Rollup(_) => {
            return Err(SqlParseError::new("ROLLUP is not supported"));
        }
        SqlExpr::Tuple(_) => {
            return Err(SqlParseError::new("ROW/TUPLE is not supported"));
        }
        SqlExpr::ArrayIndex { .. } => {
            return Err(SqlParseError::new("Array Index is not supported"));
        }
        SqlExpr::Array(_) => {
            return Err(SqlParseError::new("Array Index is not supported"));
        }
        SqlExpr::InUnnest { .. } => {
            return Err(SqlParseError::new("UNNEST is not supported"));
        }
        SqlExpr::JsonAccess { .. } => {
            return Err(SqlParseError::new("json accessors not supported"));
        }
        SqlExpr::CompositeAccess { .. } => {
            return Err(SqlParseError::new("composite accessors not supported"));
        }
        SqlExpr::IsFalse(_) => {
            return Err(SqlParseError::new("IS FALSE is not supported"));
        }
        SqlExpr::IsTrue(_) => {
            return Err(SqlParseError::new("IS TRUE is not supported"));
        }
        SqlExpr::AnyOp(_) => {
            return Err(SqlParseError::new("ANY(x) is not supported"));
        }
        SqlExpr::AllOp(_) => {
            return Err(SqlParseError::new("ALL(x) is not supported"));
        }
        SqlExpr::Position { .. } => {
            return Err(SqlParseError::new("POSITION is not supported"));
        }
        SqlExpr::AtTimeZone { .. } => {
            return Err(SqlParseError::new("AT TIME ZONE is not supported"));
        }
        SqlExpr::IsNotFalse(inner) => {
            let expr = build_expr(*inner)?;
            Expr::neq(expr, Value::Bool(false))
        }
        SqlExpr::IsNotTrue(inner) => {
            let expr = build_expr(*inner)?;
            Expr::neq(expr, Value::Bool(true))
        }
        SqlExpr::IsUnknown(_) => {
            return Err(SqlParseError::new("IS UNKNOWN is not supported"));
        }
        SqlExpr::IsNotUnknown(_) => {
            return Err(SqlParseError::new("IS NOT UNKNOWN is not supported"));
        }
        SqlExpr::Like {
            negated,
            expr,
            pattern,
            escape_char: _,
        } => {
            let left = build_expr(*expr)?;
            let right = build_expr(*pattern)?;
            let expr = Expr::contains(left, right);
            if negated {
                Expr::not(expr)
            } else {
                expr
            }
        }
        SqlExpr::ILike {
            negated,
            expr,
            pattern,
            escape_char: _,
        } => {
            let left = build_expr(*expr)?;
            let right = build_expr(*pattern)?;
            let expr = Expr::contains(left, right);
            if negated {
                Expr::not(expr)
            } else {
                expr
            }
        }
        SqlExpr::SimilarTo { .. } => {
            return Err(SqlParseError::new("SIMILAR TO is not supported"));
        }
        SqlExpr::SafeCast { .. } => {
            return Err(SqlParseError::new("SAFE CAST is not supported"));
        }
        SqlExpr::ArraySubquery(_) => {
            return Err(SqlParseError::new("ARRAY() subquery is not supported"));
        }
    };

    Ok(e)
}

fn build_delete(del: SqlDelete) -> Result<MutateSelect, SqlParseError> {
    validate_table_factor(&del.table_name)?;
    if del.using.is_some() {
        return Err(SqlParseError::new("USING is not supported"));
    }
    let selection = del
        .selection
        .ok_or_else(|| SqlParseError::new("DELETE must have a WHERE clause"))?;
    let filter = build_expr(selection)?;
    Ok(MutateSelect {
        filter,
        variables: Default::default(),
        action: MutateSelectAction::Delete,
    })
}

fn build_update(up: SqlUpdate) -> Result<MutateSelect, SqlParseError> {
    validate_table_with_joins(&up.table)?;
    if up.from.is_some() {
        return Err(SqlParseError::new("FROM is not supported"));
    }
    let selection = up
        .selection
        .ok_or_else(|| SqlParseError::new("UPDATE must have a WHERE clause"))?;
    let filter = build_expr(selection)?;

    let mut patch = Patch::new();

    for assign in up.assignments {
        let path: Vec<PatchPathElem> = assign
            .id
            .into_iter()
            .map(|ident| PatchPathElem::Key(ident.value))
            .collect();
        let value_expr = build_expr(assign.value)?;
        let value = match value_expr {
            Expr::Literal(lit) => lit,
            _other => {
                return Err(SqlParseError::new(
                    "UPDATE assignments only support literal values for now (field = LITERAL)",
                ));
            }
        };
        patch = patch.replace(path, value);
    }

    Ok(MutateSelect {
        filter,
        variables: Default::default(),
        action: MutateSelectAction::Patch(patch),
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        data::Id,
        schema::{
            builtin::{AttrId, AttrIdent, AttrTitle, AttrType},
            AttributeMeta,
        },
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

        assert_eq!(
            parse_select(r#" SELECT * FROM entities WHERE '92a2e6d5-5b45-4356-8b36-2b6127df5a58'::uuid = ANY("my/attr" ) "#).unwrap(),
            Select::new().with_filter(Expr::in_(Id::from_str("92a2e6d5-5b45-4356-8b36-2b6127df5a58").unwrap(), Expr::attr_ident("my/attr")))
        );
    }

    #[test]
    fn test_sql_parse_delete() {
        let m1 = parse_sql(r#"DELETE FROM entities WHERE "factor/id" = 42"#).unwrap();
        assert_eq!(
            m1.as_mutate().unwrap(),
            &MutateSelect {
                filter: Expr::eq(AttrId::expr(), 42u64),
                variables: Default::default(),
                action: MutateSelectAction::Delete,
            }
        );
    }

    #[test]
    fn test_sql_parse_update() {
        let m1 =
            parse_sql(r#"UPDATE entities SET "factor/title" = 'hello' WHERE "factor/id" = 42"#)
                .unwrap();
        assert_eq!(
            m1.as_mutate().unwrap(),
            &MutateSelect {
                filter: Expr::eq(AttrId::expr(), 42u64),
                variables: Default::default(),
                action: MutateSelectAction::Patch(Patch::new().replace("factor/title", "hello")),
            }
        );
    }
}
