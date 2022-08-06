use thiserror::Error;

/// Errors encountered in the store layer.
#[derive(Error, Debug)]
pub enum StoreError {
    #[error("node is not a leader")]
    NotLeader,

    #[error("opening database failed")]
    FailedDatabaseOpen,

    #[error("failed putting key-value pair into database")]
    FailedPut,

    #[error("internal error when getting value")]
    FailedGet,

    #[error("value with key not found")]
    NotFound,
}
