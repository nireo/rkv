use rocksdb::{DB, Options};
use anyhow::Result;
use std::sync::Mutex;

#[derive(Debug)]
pub struct KvStore {
    pub db: Mutex<DB>,
}

impl KvStore {
    fn new(path: &str) -> Result<KvStore> {
        let db = DB::open_default(path)?;
        Ok(KvStore{
            db: Mutex::new(db),
        })
    }
}
