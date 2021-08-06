use crate::{
    data::{Ident, Value},
    schema::AttributeDescriptor,
};

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
    Contains,
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
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

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

    pub fn and_with(self, other: impl Into<Self>) -> Self {
        Self::and(self, other.into())
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

/// Try to resolve an expression as a simple id or ident select.
pub(crate) fn expr_is_entity_ident(expr: &Expr) -> Option<Ident> {
    use crate::schema::builtin::{AttrId, AttrIdent, ATTR_ID, ATTR_IDENT};

    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            let left = left.as_ref();
            let right = right.as_ref();

            let (ident, literal) = match (left, right) {
                (Expr::Ident(ident), Expr::Literal(value)) => (ident, value),
                (Expr::Literal(value), Expr::Ident(ident)) => (ident, value),
                _ => {
                    return None;
                }
            };

            match ident {
                // Id
                Ident::Id(ATTR_ID) => literal.as_id().map(Ident::from),
                Ident::Name(name) if name == AttrId::QUALIFIED_NAME => {
                    literal.as_id().map(Ident::from)
                }
                // Ident.
                Ident::Id(ATTR_IDENT) => literal
                    .as_str()
                    .map(|name| Ident::Name(name.to_string().into())),
                Ident::Name(name) if name == AttrIdent::QUALIFIED_NAME => literal
                    .as_str()
                    .map(|name| Ident::Name(name.to_string().into())),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        schema::builtin::{AttrId, AttrIdent, ATTR_ID, ATTR_IDENT},
        Id,
    };

    use super::*;

    #[test]
    fn test_expr_is_entity_ident() {
        let nil_ident = Ident::from(Id::nil());

        // ID.

        let a = Expr::eq(
            Expr::Ident(ATTR_ID.into()),
            Expr::Literal(Value::Id(Id::nil())),
        );
        assert_eq!(Some(nil_ident.clone()), expr_is_entity_ident(&a));

        let a = Expr::eq(
            Expr::Literal(Value::Id(Id::nil())),
            Expr::Ident(ATTR_ID.into()),
        );
        assert_eq!(Some(nil_ident.clone()), expr_is_entity_ident(&a));

        let a = Expr::eq(
            Expr::Ident(AttrId::IDENT),
            Expr::Literal(Value::Id(Id::nil())),
        );
        assert_eq!(Some(nil_ident.clone()), expr_is_entity_ident(&a));

        let a = Expr::eq(
            Expr::Literal(Value::Id(Id::nil())),
            Expr::Ident(AttrId::IDENT),
        );
        assert_eq!(Some(nil_ident.clone()), expr_is_entity_ident(&a));

        // IDENT.
        //
        let hello_value = Value::from("hello");
        let hello_ident = Ident::from("hello");

        let a = Expr::eq(
            Expr::Ident(ATTR_IDENT.into()),
            Expr::Literal(hello_value.clone()),
        );
        assert_eq!(Some(hello_ident.clone()), expr_is_entity_ident(&a));

        let a = Expr::eq(
            Expr::Literal(hello_value.clone()),
            Expr::Ident(ATTR_IDENT.into()),
        );
        assert_eq!(Some(hello_ident.clone()), expr_is_entity_ident(&a));

        let a = Expr::eq(
            Expr::Ident(AttrIdent::IDENT),
            Expr::Literal(hello_value.clone()),
        );
        assert_eq!(Some(hello_ident.clone()), expr_is_entity_ident(&a));

        let a = Expr::eq(
            Expr::Literal(hello_value.clone()),
            Expr::Ident(AttrIdent::IDENT),
        );
        assert_eq!(Some(hello_ident.clone()), expr_is_entity_ident(&a));
    }
}
