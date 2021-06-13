use crate::data::{Ident, Value};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    And,
    Or,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    In(Vec<Value>),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    Literal(Value),
    Ident(Ident),
    Variable(String),
    UnaryOp {
        op: UnaryOp,
        expr: Box<Self>,
    },
    BinaryOp {
        left: Box<Self>,
        op: BinaryOp,
        right: Box<Self>,
    },
    If {
        value: Box<Self>,
        then: Box<Self>,
        or: Box<Self>,
    },
}

impl Expr {
    pub fn literal<I>(value: I) -> Self
    where
        I: Into<Value>,
    {
        Self::Literal(value.into())
    }

    pub fn ident<I>(value: I) -> Self
    where
        I: Into<Ident>,
    {
        Self::Ident(value.into())
    }

    pub fn var<S>(name: S) -> Self
    where
        S: Into<String>,
    {
        Self::Variable(name.into())
    }

    pub fn unary<I>(op: UnaryOp, expr: I) -> Self
    where
        I: Into<Self>,
    {
        Self::UnaryOp {
            op,
            expr: Box::new(expr.into()),
        }
    }

    pub fn not<I>(expr: I) -> Self
    where
        I: Into<Self>,
    {
        Self::unary(UnaryOp::Not, expr)
    }

    pub fn binary<I1, I2>(left: I1, op: BinaryOp, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::BinaryOp {
            left: Box::new(left.into()),
            op,
            right: Box::new(right.into()),
        }
    }

    pub fn and<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::BinaryOp {
            left: Box::new(left.into()),
            op: BinaryOp::And,
            right: Box::new(right.into()),
        }
    }

    pub fn or<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::BinaryOp {
            left: Box::new(left.into()),
            op: BinaryOp::Or,
            right: Box::new(right.into()),
        }
    }

    pub fn eq<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::BinaryOp {
            left: Box::new(left.into()),
            op: BinaryOp::Eq,
            right: Box::new(right.into()),
        }
    }

    pub fn neq<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::BinaryOp {
            left: Box::new(left.into()),
            op: BinaryOp::Neq,
            right: Box::new(right.into()),
        }
    }

    pub fn is_null<I>(expr: I) -> Self
    where
        I: Into<Self>,
    {
        Self::eq(expr, Self::Literal(Value::Unit))
    }

    pub fn is_not_null<I>(expr: I) -> Self
    where
        I: Into<Self>,
    {
        Self::neq(expr, Self::Literal(Value::Unit))
    }
}

impl<V> From<V> for Expr
where
    V: Into<Value>,
{
    fn from(v: V) -> Self {
        Self::Literal(v.into())
    }
}
