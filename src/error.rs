use thiserror::Error;

/// Errors encountered in the store layer.
#[derive(Error, Debug)]
pub enum StoreError {
    #[error("node is not a leader")]
    NotLeader,
}
