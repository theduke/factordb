#[cfg(feature = "memory")]
pub mod memory;

#[cfg(feature = "log")]
pub mod log;

use crate::{
    registry::{LocalIndexId, SharedRegistry},
    util::VecSet,
};
use factordb::{
    data::{patch::Patch, DataMap, Id, IdOrIdent, Value},
    query::{self, expr::Expr, migrate::Migration, select::Item},
    schema, AnyError,
};

pub type BackendFuture<T> = futures::future::BoxFuture<'static, Result<T, AnyError>>;

pub trait Dao: Send + 'static {
    fn get(&self, attr: &schema::Attribute) -> Result<Option<Value>, AnyError>;

    fn get_opt(&self, attr: &schema::Attribute) -> Option<Value> {
        self.get(attr).ok().flatten()
    }

    fn set(&mut self, attr: &schema::Attribute, value: Value);

    // fn into_data_map(self) -> DataMap;
}

pub trait Backend {
    fn registry(&self) -> &SharedRegistry;

    fn entity(&self, id: IdOrIdent) -> BackendFuture<Option<DataMap>>;
    fn select(&self, query: query::select::Select) -> BackendFuture<query::select::Page<Item>>;

    fn select_map(&self, query: query::select::Select) -> BackendFuture<Vec<DataMap>>;

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
    pub data: DataMap,
    pub index_ops: Vec<TupleIndexInsert>,
}

impl From<TupleCreate> for TupleAction {
    fn from(v: TupleCreate) -> Self {
        Self::Create(v)
    }
}

#[derive(Clone, Debug)]
pub struct TupleReplace {
    pub data: DataMap,
    pub index_ops: Vec<TupleIndexOp>,
}

#[derive(Clone, Debug)]
pub struct TupleMerge {
    pub data: DataMap,
    pub index_ops: Vec<TupleIndexOp>,
}

#[derive(Clone, Debug)]
pub struct TuplePatch {
    pub patch: Patch,
    pub index_ops: Vec<TupleIndexOp>,
}

#[derive(Clone, Debug)]
pub struct TupleDelete {
    pub index_ops: Vec<TupleIndexRemove>,
}

#[derive(Clone, Debug)]
pub struct TupleRemoveAttrs {
    pub attrs: Vec<Id>,
    pub index_ops: Vec<TupleIndexRemove>,
}

#[derive(Clone, Debug)]
pub enum TupleAction {
    Create(TupleCreate),
    Replace(TupleReplace),
    Merge(TupleMerge),
    Patch(TuplePatch),
    RemoveAttrs(TupleRemoveAttrs),
    Delete(TupleDelete),
}

impl From<TupleMerge> for TupleAction {
    fn from(v: TupleMerge) -> Self {
        Self::Merge(v)
    }
}

impl From<TupleReplace> for TupleAction {
    fn from(v: TupleReplace) -> Self {
        Self::Replace(v)
    }
}

impl From<TuplePatch> for TupleAction {
    fn from(v: TuplePatch) -> Self {
        Self::Patch(v)
    }
}

impl From<TupleDelete> for TupleAction {
    fn from(v: TupleDelete) -> Self {
        Self::Delete(v)
    }
}

impl From<TupleRemoveAttrs> for TupleAction {
    fn from(v: TupleRemoveAttrs) -> Self {
        Self::RemoveAttrs(v)
    }
}

#[derive(Clone, Debug)]
pub struct TupleOp {
    pub target: IdOrIdent,
    pub action: TupleAction,
}

impl TupleOp {
    pub fn new<A: Into<TupleAction>, I: Into<IdOrIdent>>(target: I, action: A) -> Self {
        Self {
            target: target.into(),
            action: action.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SelectOp {
    pub selector: Expr,
    // FIXME: use dedicated type to prevent the reundant id field...
    pub action: TupleAction,
}

impl SelectOp {
    pub fn new<I: Into<TupleAction>>(selector: Expr, action: I) -> Self {
        Self {
            selector,
            action: action.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexPopulate {
    pub index_id: Id,
}

/// Validate that an entity with the given id exists.
#[derive(Clone, Debug)]
pub struct ValidateEntityExists {
    pub id: Id,
}

/// Validate that an entity with the given id exists and has the specified type(s).
#[derive(Clone, Debug)]
pub struct ValidateEntityType {
    pub id: Id,
    // TODO: this should be an Arc<_> to prevent cloning overhead
    // TODO: use LocalEntityId instead of string id?
    pub allowed_types: VecSet<Id>,
}

#[derive(Clone, Debug)]
pub enum DbOp {
    ValidateEntityExists(ValidateEntityExists),
    ValidateEntityType(ValidateEntityType),
    Tuple(TupleOp),
    Select(SelectOp),
    IndexPopulate(IndexPopulate),
}

impl DbOp {
    pub fn new_validate_entity_exists(id: Id) -> Self {
        Self::ValidateEntityExists(ValidateEntityExists { id })
    }
}
