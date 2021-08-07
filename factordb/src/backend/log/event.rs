use crate::query::{migrate::Migration, mutate::BatchUpdate};

/// A event persisted in the log.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LogEvent {
    pub(super) id: super::EventId,
    pub(super) op: LogOp,
}

impl LogEvent {
    // /// Get a reference to the log event's id.
    // fn id(&self) -> EventId {
    //     self.id
    // }

    // fn from_op(op: super::DbOp) -> Option<Self> {
    //     use super::{DbOp, TupleOp};
    //     match op {
    //         DbOp::Tuple(t) => match t {
    //             TupleOp::Create(_) => todo!(),
    //             TupleOp::Replace(_) => todo!(),
    //             TupleOp::Merge(_) => todo!(),
    //             TupleOp::Delete(_) => todo!(),
    //             TupleOp::RemoveAttrs(_) => todo!(),
    //         },
    //         DbOp::Select(_) => todo!(),
    //     }
    // }
}

/// A log operation stored in a log event.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub(super) enum LogOp {
    Batch(BatchUpdate),
    Migrate(Migration),
}
