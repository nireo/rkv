use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender};
use derivative::Derivative;
use little_raft::{
    cluster::Cluster,
    message::Message,
    replica::{Replica, ReplicaID},
    state_machine::{Snapshot, StateMachine, StateMachineTransition, TransitionState},
};
use rocksdb::DB;
use std::collections::HashMap;
use tokio::sync::Notify;

use crate::{rpc::proto::PutResponse, error::StoreError};

const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(500);
const MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(750);
const MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(950);

#[derive(Debug, Clone)]
pub struct StoreCommand {
    pub id: usize,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl StateMachineTransition for StoreCommand {
    type TransitionID = usize;

    fn get_id(&self) -> Self::TransitionID {
        self.id
    }
}

#[async_trait]
pub trait StoreTransport {
    fn send(&self, to_id: usize, msg: Message<StoreCommand, Bytes>);

    async fn delegate(
        &self,
        to_id: usize,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<PutResponse, StoreError>;
}

#[derive(Debug)]
struct StoreConfig {
    pub path: String,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Store<T: StoreTransport + Send + Sync> {
    this_id: usize,
    leader: Option<usize>,
    leader_exists: AtomicBool,
    waiters: Vec<Arc<Notify>>,
    pending_messages: Vec<Message<StoreCommand, Bytes>>,
    transport: Arc<T>,
    pending_transitions: Vec<StoreCommand>,
    command_completions: HashMap<u64, Arc<Notify>>,
    results: HashMap<u64, Result<PutResponse, StoreError>>,

    #[derivative(Debug = "ignore")]
    connection: Arc<Mutex<DB>>,
}

impl<T: StoreTransport + Send + Sync> Store<T> {
    pub fn new(this_id: usize, transport: T, config: StoreConfig) -> Result<Self> {
        let db = DB::open_default(config.path)?;

        Ok(Store {
            this_id,
            leader: None,
            leader_exists: AtomicBool::new(false),
            waiters: Vec::new(),
            pending_messages: Vec::new(),
            transport: Arc::new(transport),
            pending_transitions: Vec::new(),
            command_completions: HashMap::new(),
            results: HashMap::new(),
            connection: Arc::new(Mutex::new(db)),
        })
    }

    pub fn is_leader(&self) -> bool {
        match self.leader {
            Some(id) => id == self.this_id,
            _ => false,
        }
    }
}

impl<T: StoreTransport + Send + Sync> StateMachine<StoreCommand, Bytes> for Store<T> {
    fn register_transition_state(&mut self, transition_id: usize, state: TransitionState) {
        match state {
            TransitionState::Applied | TransitionState::Abandoned(_) => {
                if let Some(completion) = self.command_completions.remove(&(transition_id as u64)) {
                    completion.notify_one()
                }
            }
            _ => (),
        }
    }

    fn apply_transition(&mut self, transition: StoreCommand) {
        if transition.id == 0 {
            return;
        }
        // TODO: add apply
    }

    fn get_pending_transitions(&mut self) -> Vec<StoreCommand> {
        let cur = self.pending_transitions.clone();
        self.pending_transitions = Vec::new();
        cur
    }

    fn get_snapshot(&mut self) -> Option<Snapshot<Bytes>> {
        todo!("Snapshotting is not implemented.");
    }

    fn create_snapshot(&mut self, _index: usize, _term: usize) -> Snapshot<Bytes> {
        todo!("Snapshotting is not implemented.");
    }

    fn set_snapshot(&mut self, _snapshot: Snapshot<Bytes>) {
        todo!("Snapshotting is not implemented.");
    }
}

impl<T: StoreTransport + Send + Sync> Cluster<StoreCommand, Bytes> for Store<T> {
    fn register_leader(&mut self, leader_id: Option<ReplicaID>) {
        if let Some(id) = leader_id {
            self.leader = Some(id);
            self.leader_exists.store(true, Ordering::SeqCst);
        } else {
            self.leader = None;
            self.leader_exists.store(false, Ordering::SeqCst);
        }
        let waiters = self.waiters.clone();
        self.waiters = Vec::new();
        for waiter in waiters {
            waiter.notify_one();
        }
    }

    fn send_message(&mut self, to_id: usize, message: Message<StoreCommand, Bytes>) {
        self.transport.send(to_id, message);
    }

    fn receive_messages(&mut self) -> Vec<Message<StoreCommand, Bytes>> {
        let cur = self.pending_messages.clone();
        self.pending_messages = Vec::new();
        cur
    }

    fn halt(&self) -> bool {
        false
    }
}

type StoreReplica<T> = Replica<Store<T>, Store<T>, StoreCommand, Bytes>;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StoreServer<T: StoreTransport + Send + Sync> {
    next_cmd_in: AtomicU64,
    store: Arc<Mutex<Store<T>>>,
    #[derivative(Debug = "ignore")]
    replica: Arc<Mutex<StoreReplica<T>>>,
    message_notifier_rx: Receiver<()>,
    message_notifier_tx: Sender<()>,
    transition_notifier_rx: Receiver<()>,
    transition_notifier_tx: Sender<()>,
}

impl<T: StoreTransport + Send + Sync> StoreServer<T> {
    pub fn start(this_id: usize, peers: Vec<usize>, transport: T) -> Result<Self, StoreError> {
        let config = StoreConfig {
            path: "./rkv".to_string(),
        };
        let store = Arc::new(Mutex::new(Store::new(this_id, transport, config)));
        let noop = StoreCommand {
            id: 0,
            key: vec![],
            value: vec![],
        };

        let (message_notifier_tx, message_notifier_rx) = crossbeam::channel::unbounded();
        let (transition_notifier_tx, transition_notifier_rx) = crossbeam::channel::unbounded();

        let replica = Replica::new(
            this_id,
            peers,
            store.clone(),
            store.clone(),
            0,
            noop,
            HEARTBEAT_TIMEOUT,
            (MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT),
        );

        let replica = Arc::new(Mutex::new(replica));
        Ok(StoreServer {
            next_cmd_in: AtomicU64::new(1),
            store,
            replica,
            message_notifier_rx,
            message_notifier_tx,
            transition_notifier_rx,
            transition_notifier_tx,
        })
    }
}
