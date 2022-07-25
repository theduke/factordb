use anyhow::bail;
use mongodb_language_model::{Clause, Expression, Operator, Value as MongoValue};

use crate::prelude::{Value, ValueMap};

use super::expr::{BinaryOp, Expr};

pub fn parse_mongo_query(input: &str) -> Result<Option<Expr>, anyhow::Error> {
    // Workaround for parser library returning an error for an empty query '{}'.
    // See https://github.com/fcoury/mongodb-language-model-rust/issues/2
    if input
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        == "{}"
    {
        return Ok(None);
    }

    let parsed = mongodb_language_model::parse(input)?;

    let mut clauses = parsed
        .clauses
        .into_iter()
        .map(|clause| parse_clause(clause))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten();

    let first = if let Some(f) = clauses.next() {
        f
    } else {
        return Ok(None);
    };

    let finished = clauses.fold(first, |left, right| {
        Expr::binary(left, BinaryOp::And, right)
    });

    Ok(Some(finished))
}

fn parse_clause(clause: Clause) -> Result<Option<Expr>, anyhow::Error> {
    match clause {
        Clause::Leaf(leaf) => match leaf.value {
            MongoValue::Leaf(value) => Ok(Some(Expr::binary(
                Expr::attr_ident(&leaf.key),
                BinaryOp::Eq,
                Expr::literal(parse_json_value(value.value)?),
            ))),
            MongoValue::Operators(ops) => {
                let left = Expr::attr_ident(&leaf.key);

                let mut clauses = ops
                    .into_iter()
                    .map(|op| parse_operator(left.clone(), op))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten();

                let first = if let Some(f) = clauses.next() {
                    f
                } else {
                    return Ok(None);
                };

                let finished = clauses.fold(first, |left, right| {
                    Expr::binary(left, BinaryOp::And, right)
                });

                Ok(Some(finished))
            }
        },
        Clause::ExpressionTree(tree) => match tree.operator.as_str() {
            "$or" => {
                let exprs = tree
                    .expressions
                    .into_iter()
                    .map(|expr| parse_expression(expr).transpose())
                    .flatten()
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Expr::or_iter(exprs))
            }
            "$and" => {
                let exprs = tree
                    .expressions
                    .into_iter()
                    .map(|expr| parse_expression(expr).transpose())
                    .flatten()
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Expr::and_iter(exprs))
            }
            other => bail!("Unsupported operator: '{}'", other),
        },
    }
}

fn parse_expression(expr: Expression) -> Result<Option<Expr>, anyhow::Error> {
    let exprs = expr
        .clauses
        .into_iter()
        .map(|clause| parse_clause(clause).transpose())
        .flatten()
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Expr::and_iter(exprs))
}

fn parse_operator(left: Expr, op: Operator) -> Result<Option<Expr>, anyhow::Error> {
    match op {
        Operator::Value(valop) => {
            let right = parse_json_value(valop.value.value)?;

            match valop.operator.as_str() {
                "$eq" => Ok(Some(Expr::binary(left, BinaryOp::Eq, right))),
                "$ne" => Ok(Some(Expr::binary(left, BinaryOp::Neq, right))),
                "$gt" => Ok(Some(Expr::binary(left, BinaryOp::Gt, right))),
                "$gte" => Ok(Some(Expr::binary(left, BinaryOp::Gte, right))),
                "$lt" => Ok(Some(Expr::binary(left, BinaryOp::Lt, right))),
                "$lte" => Ok(Some(Expr::binary(left, BinaryOp::Lte, right))),
                "$in" => Ok(Some(Expr::binary(left, BinaryOp::In, right))),
                "$nin" => Ok(Some(Expr::not(Expr::binary(left, BinaryOp::In, right)))),
                other => {
                    bail!("Unsupported value operator: '{}'", other)
                }
            }
        }
        Operator::List(list) => match list.operator.as_str() {
            "$in" => {
                let values = list
                    .values
                    .into_iter()
                    .map(|v| parse_json_value(v.value).map(Expr::Literal))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Some(Expr::in_(left, Expr::List(values))))
            }
            "$nin" => {
                let values = list
                    .values
                    .into_iter()
                    .map(|v| parse_json_value(v.value).map(Expr::Literal))
                    .collect::<Result<Vec<_>, _>>()?;
                let in_ = Expr::in_(left, Expr::List(values));
                Ok(Some(Expr::not(in_)))
            }
            other => {
                bail!("Unsupported list operator: '{}'", other)
            }
        },
        Operator::ExpressionOperator(_op) => todo!(),
    }
}

fn parse_json_value(value: serde_json::Value) -> Result<Value, anyhow::Error> {
    let v = match value {
        serde_json::Value::Null => Value::Unit,
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(num) => {
            if let Some(v) = num.as_u64() {
                Value::UInt(v)
            } else if let Some(v) = num.as_i64() {
                Value::Int(v)
            } else if let Some(v) = num.as_f64() {
                Value::Float(v.into())
            } else {
                bail!("Unsupported numeric value: {num}");
            }
        }
        serde_json::Value::String(v) => Value::String(v),
        serde_json::Value::Array(values) => Value::List(
            values
                .into_iter()
                .map(|v| parse_json_value(v))
                .collect::<Result<_, _>>()?,
        ),
        serde_json::Value::Object(map) => {
            let items = map
                .into_iter()
                .map(|(k, v)| -> Result<_, anyhow::Error> {
                    Ok((Value::String(k), parse_json_value(v)?))
                })
                .collect::<Result<ValueMap<Value>, _>>()?;
            Value::Map(items)
        }
    };

    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_mongo_query() {
        assert_eq!(
            parse_mongo_query(r#"{ "a": 1 }"#).unwrap().unwrap(),
            Expr::eq(Expr::attr_ident("a"), 1u64),
        );

        assert_eq!(
            parse_mongo_query(r#"{ "a": 1, "b": true }"#)
                .unwrap()
                .unwrap(),
            Expr::and(
                Expr::eq(Expr::attr_ident("a"), 1u64),
                Expr::eq(Expr::attr_ident("b"), true),
            )
        );

        assert_eq!(
            parse_mongo_query(
                r#"{ 
                    "a": {"$eq": 1},
                    "a": {"$ne": 1},
                    "a": {"$lt": 1},
                    "a": {"$lte": 1}, "a": {"$gt": 1},
                    "a": {"$gte": 1},
                    "a": {"$in": [1]},
                    "a": {"$nin": [2, -4]} 
            }"#
            )
            .unwrap()
            .unwrap(),
            Expr::and_iter([
                Expr::eq(Expr::attr_ident("a"), 1u64),
                Expr::neq(Expr::attr_ident("a"), 1u64),
                Expr::lt(Expr::attr_ident("a"), 1u64),
                Expr::lte(Expr::attr_ident("a"), 1u64),
                Expr::gt(Expr::attr_ident("a"), 1u64),
                Expr::gte(Expr::attr_ident("a"), 1u64),
                Expr::in_(Expr::attr_ident("a"), Expr::List(vec![1u64.into()])),
                Expr::not(Expr::in_(
                    Expr::attr_ident("a"),
                    Expr::List(vec![2u64.into(), (-4i64).into()])
                )),
            ])
            .unwrap(),
        );

        assert_eq!(parse_mongo_query(r#"{ "$or": [] }"#).unwrap(), None,);

        assert_eq!(
            parse_mongo_query(r#"{ "$or": [{"a": 1}] }"#)
                .unwrap()
                .unwrap(),
            Expr::eq(Expr::attr_ident("a"), 1u64)
        );

        assert_eq!(
            parse_mongo_query(r#"{ "$or": [{"a": 1}, {"b": {"$gt": 2}}] }"#)
                .unwrap()
                .unwrap(),
            Expr::or(
                Expr::eq(Expr::attr_ident("a"), 1u64),
                Expr::gt(Expr::attr_ident("b"), 2u64),
            )
        );

        assert_eq!(
            parse_mongo_query(r#"{ "$and": [{"a": 1}, {"b": {"$gt": 2}}] }"#)
                .unwrap()
                .unwrap(),
            Expr::and(
                Expr::eq(Expr::attr_ident("a"), 1u64),
                Expr::gt(Expr::attr_ident("b"), 2u64),
            )
        );
    }
}
