pub mod log;
pub mod memory;

use crate::{
    data::{DataMap, Id, Ident, Value},
    query::{self, expr::Expr, select::Item},
    registry::SharedRegistry,
    schema, AnyError,
};

pub type BackendFuture<T> = futures::future::BoxFuture<'static, Result<T, AnyError>>;

pub trait Dao: Send + 'static {
    fn get(&self, attr: &schema::AttributeSchema) -> Result<Option<Value>, AnyError>;

    fn get_opt(&self, attr: &schema::AttributeSchema) -> Option<Value> {
        self.get(attr).ok().flatten()
    }

    fn set(&mut self, attr: &schema::AttributeSchema, value: Value);

    // fn into_data_map(self) -> DataMap;
}

pub trait Backend {
    fn registry(&self) -> &SharedRegistry;

    fn entity(&self, id: Ident) -> BackendFuture<DataMap>;
    fn select(&self, query: query::select::Select) -> BackendFuture<query::select::Page<Item>>;

    fn apply_batch(&self, batch: query::update::BatchUpdate) -> BackendFuture<()>;
    fn migrate(&self, migration: query::migrate::Migration) -> BackendFuture<()>;

    fn purge_all_data(&self) -> BackendFuture<()>;

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }
}

// pub enum SchemaOp {
//     AttrCreate(AttributeSchema),
//     AttrDelete(Id),
//     EntityCreate(EntitySchema),
//     EntityDelete(Id),
// }

// pub struct TuplePersist {
//     pub id: Option<Id>,
//     pub ident: Option<Ident>,
//     pub data: FnvHashMap<Id, Value>,
//     pub create: Option<bool>,
// }

// pub struct Migration {
//     pub ops: Vec<SchemaOp>,
//     pub persist: Vec<TuplePersist>,
// }

#[derive(Clone, Debug)]
pub struct TupleCreate {
    pub id: Id,
    pub data: DataMap,
}

#[derive(Clone, Debug)]
pub struct TupleReplace {
    pub id: Id,
    pub data: DataMap,
}

#[derive(Clone, Debug)]
pub struct TupleMerge {
    pub id: Id,
    pub data: DataMap,
}

#[derive(Clone, Debug)]
pub struct TupleDelete {
    pub id: Id,
}

#[derive(Clone, Debug)]
pub struct TupleRemoveAttrs {
    pub id: Id,
    pub attrs: Vec<Id>,
}

#[derive(Clone, Debug)]
pub enum TupleOp {
    Create(TupleCreate),
    Replace(TupleReplace),
    Merge(TupleMerge),
    RemoveAttrs(TupleRemoveAttrs),
    Delete(TupleDelete),
}

#[derive(Clone, Debug)]
pub struct SelectOpt {
    pub selector: Expr,
    /// Reusing TupleOp for convenience.
    /// Note that the Id on these TupleOps is always nil.
    pub op: TupleOp,
}

#[derive(Clone, Debug)]
pub enum DbOp {
    Tuple(TupleOp),
    Select(SelectOpt),
}
