use anyhow::Result;
use rocksdb::DB;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct KvStore {
    pub db: Arc<Mutex<DB>>,
}

pub enum DbOp {
    Put { key: Vec<u8>, val: Vec<u8> },
    Get { key: Vec<u8> },
}

impl KvStore {
    pub fn new(path: &str) -> Result<KvStore> {
        let db = DB::open_default(path)?;
        Ok(KvStore {
            db: Arc::new(Mutex::new(db)),
        })
    }
}
