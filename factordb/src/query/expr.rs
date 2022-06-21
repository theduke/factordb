use crate::{
    data::{IdOrIdent, Value},
    schema::{builtin::AttrType, AttributeDescriptor, EntityDescriptor},
};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub enum BinaryOp {
    And,
    Or,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Contains,
    RegexMatch,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub enum UnaryOp {
    Not,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub enum Expr {
    /// Match entities that either match the given entity type or inherit from
    /// it.
    InheritsEntityType(String),
    Literal(Value),
    List(Vec<Self>),
    /// Select the value of an attribute.
    Attr(IdOrIdent),
    /// Resolve the value of an [`Ident`] into an [`Id`].
    Ident(IdOrIdent),
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
    pub fn as_literal(&self) -> Option<&Value> {
        if let Self::Literal(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub fn attr<A: AttributeDescriptor>() -> Self {
        Self::Attr(A::IDENT)
    }

    pub fn attr_ident(value: &str) -> Self {
        Self::Attr(IdOrIdent::Name(value.to_string().into()))
    }

    pub fn literal<I>(value: I) -> Self
    where
        I: Into<Value>,
    {
        Self::Literal(value.into())
    }

    pub fn ident<I>(value: I) -> Self
    where
        I: Into<IdOrIdent>,
    {
        Self::Attr(value.into())
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

    pub fn regex_match<I1, I2>(left: I1, regex: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<String>,
    {
        Self::binary(
            left,
            BinaryOp::RegexMatch,
            Expr::Literal(Value::String(regex.into())),
        )
    }

    pub fn in_<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::In, right)
    }

    pub fn contains<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::Contains, right)
    }

    pub fn and<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::And, right)
    }

    pub fn and_with(self, other: impl Into<Self>) -> Self {
        Self::and(self, other.into())
    }

    pub fn or<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::Or, right)
    }

    pub fn or_with(self, other: impl Into<Self>) -> Self {
        Self::or(self, other.into())
    }

    pub fn eq<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::Eq, right)
    }

    pub fn neq<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::Neq, right)
    }

    pub fn gt<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::Gt, right)
    }

    pub fn gte<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::Gte, right)
    }

    pub fn lt<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::Lt, right)
    }

    pub fn lte<I1, I2>(left: I1, right: I2) -> Self
    where
        I1: Into<Self>,
        I2: Into<Self>,
    {
        Self::binary(left, BinaryOp::Lte, right)
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

    pub fn is_entity<T: EntityDescriptor>() -> Self {
        Self::eq(Expr::attr::<AttrType>(), T::QUALIFIED_NAME)
    }

    pub fn is_entity_nested<T: EntityDescriptor>() -> Self {
        Self::InheritsEntityType(T::QUALIFIED_NAME.to_string())
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
