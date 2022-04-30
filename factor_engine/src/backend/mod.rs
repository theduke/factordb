pub mod query_planner;

#[cfg(feature = "memory")]
pub mod memory;

#[cfg(feature = "log")]
pub mod log;

use crate::registry::{LocalIndexId, SharedRegistry};
use factordb::{
    data::{patch::Patch, DataMap, Id, IdOrIdent, Value},
    query::{self, expr::Expr, migrate::Migration, select::Item},
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

    fn entity(&self, id: IdOrIdent) -> BackendFuture<Option<DataMap>>;
    fn select(&self, query: query::select::Select) -> BackendFuture<query::select::Page<Item>>;

    fn apply_batch(&self, batch: query::mutate::Batch) -> BackendFuture<()>;
    fn migrate(&self, migration: query::migrate::Migration) -> BackendFuture<()>;

    fn purge_all_data(&self) -> BackendFuture<()>;

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    fn migrations(&self) -> BackendFuture<Vec<Migration>>;

    /// The current memory usage in bytes.
    fn memory_usage(&self) -> BackendFuture<Option<u64>>;

    /// The full database size in the backing storage.
    fn storage_usage(&self) -> BackendFuture<Option<u64>>;
}

#[derive(Clone, Debug)]
pub struct TupleIndexInsert {
    pub index: LocalIndexId,
    pub value: Value,
    pub unique: bool,
}

#[derive(Clone, Debug)]
pub struct TupleIndexReplace {
    pub index: LocalIndexId,
    pub old_value: Value,
    pub value: Value,
    pub unique: bool,
}

#[derive(Clone, Debug)]
pub struct TupleIndexRemove {
    pub index: LocalIndexId,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub enum TupleIndexOp {
    Insert(TupleIndexInsert),
    Replace(TupleIndexReplace),
    Remove(TupleIndexRemove),
}

#[derive(Clone, Debug)]
pub struct TupleCreate {
    pub id: Id,
    pub data: DataMap,
    pub index_ops: Vec<TupleIndexInsert>,
}

#[derive(Clone, Debug)]
pub struct TupleReplace {
    pub id: Id,
    pub data: DataMap,
    pub index_ops: Vec<TupleIndexOp>,
}

#[derive(Clone, Debug)]
pub struct TupleMerge {
    pub id: Id,
    pub data: DataMap,
    pub index_ops: Vec<TupleIndexOp>,
}

#[derive(Clone, Debug)]
pub struct TuplePatch {
    pub id: Id,
    pub patch: Patch,
    pub index_ops: Vec<TupleIndexOp>,
}

#[derive(Clone, Debug)]
pub struct TupleDelete {
    pub id: Id,
    pub index_ops: Vec<TupleIndexRemove>,
}

#[derive(Clone, Debug)]
pub struct TupleRemoveAttrs {
    pub id: Id,
    pub attrs: Vec<Id>,
    pub index_ops: Vec<TupleIndexRemove>,
}

#[derive(Clone, Debug)]
pub enum TupleOp {
    Create(TupleCreate),
    Replace(TupleReplace),
    Merge(TupleMerge),
    Patch(TuplePatch),
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
pub struct IndexPopulate {
    pub index_id: Id,
}

#[derive(Clone, Debug)]
pub enum DbOp {
    Tuple(TupleOp),
    Select(SelectOpt),
    IndexPopulate(IndexPopulate),
}
