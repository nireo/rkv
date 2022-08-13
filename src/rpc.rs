use crate::{StoreCommand, StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;
use bytes::Bytes;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
use little_raft::message::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("kvproto");
}

use proto::rpc_client::RpcClient;
use proto::rpc_server::Rpc;
use proto::{
    AppendEntriesRequest, AppendEntriesResponse, Empty, LogEntry,
    PutRequest, PutResponse, VoteRequest, VoteResponse,
};

type NodeAddrFn = dyn Fn(usize) -> String + Send + Sync;

#[derive(Debug)]
struct ConnectionPool {
    connections: ArrayQueue<RpcClient<tonic::transport::Channel>>,
}

struct Connection {
    conn: RpcClient<tonic::transport::Channel>,
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.replenish(self.conn.clone())
    }
}

impl ConnectionPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: ArrayQueue::new(16),
        })
    }

    async fn connection<S: ToString>(&self, addr: S) -> RpcClient<tonic::transport::Channel> {
        let addr = addr.to_string();

        match self.connections.pop() {
            Some(x) => x,
            None => RpcClient::connect(addr).await.unwrap(),
        }
    }

    fn replenish(&self, conn: RpcClient<tonic::transport::Channel>) {
        let _ = self.connections.push(conn);
    }
}

#[derive(Debug, Clone)]
struct Connections(Arc<Mutex<HashMap<String, Arc<ConnectionPool>>>>);

impl Connections {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    async fn connection<S: ToString>(&self, addr: S) -> Connection {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        let pool = conns
            .entry(addr.clone())
            .or_insert_with(ConnectionPool::new);

        Connection {
            conn: pool.connection(addr).await,
            pool: pool.clone(),
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
    connections: Connections,
}

impl RpcTransport {
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport {
            node_addr,
            connections: Connections::new(),
        }
    }
}

#[async_trait]
impl StoreTransport for RpcTransport {
    fn send(&self, to_id: usize, msg: Message<StoreCommand, Bytes>) {
        match msg {
            Message::AppendEntryRequest {
                from_id,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                commit_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let prev_log_index = prev_log_index as u64;
                let prev_log_term = prev_log_term as u64;
                let entries = entries
                    .iter()
                    .map(|entry| {
                        let id = entry.transition.id as u64;
                        let index = entry.index as u64;
                        let key = entry.transition.key.clone();
                        let value = entry.transition.value.clone();
                        let term = entry.term as u64;
                        LogEntry {
                            id,
                            key,
                            value,
                            index,
                            term,
                        }
                    })
                    .collect();
                let commit_index = commit_index as u64;
                let request = AppendEntriesRequest {
                    from_id,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    commit_index,
                };
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.append_entries(request).await.unwrap();
                });
            }
            Message::AppendEntryResponse {
                from_id,
                term,
                success,
                last_index,
                mismatch_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_index = last_index as u64;
                let mismatch_index = mismatch_index.map(|idx| idx as u64);
                let request = AppendEntriesResponse {
                    from_id,
                    term,
                    success,
                    last_index,
                    mismatch_index,
                };
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client
                        .conn
                        .respond_to_append_entries(request)
                        .await
                        .unwrap();
                });
            }
            Message::VoteRequest {
                from_id,
                term,
                last_log_index,
                last_log_term,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_log_index = last_log_index as u64;
                let last_log_term = last_log_term as u64;
                let request = VoteRequest {
                    from_id,
                    term,
                    last_log_index,
                    last_log_term,
                };
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let vote = tonic::Request::new(request.clone());
                    client.conn.vote(vote).await.unwrap();
                });
            }
            Message::VoteResponse {
                from_id,
                term,
                vote_granted,
            } => {
                let peer = (self.node_addr)(to_id);
                tokio::task::spawn(async move {
                    let from_id = from_id as u64;
                    let term = term as u64;
                    let response = VoteResponse {
                        from_id,
                        term,
                        vote_granted,
                    };
                    if let Ok(mut client) = RpcClient::connect(peer.to_string()).await {
                        let response = tonic::Request::new(response.clone());
                        client.respond_to_vote(response).await.unwrap();
                    }
                });
            }
            Message::InstallSnapshotRequest { .. } => {
                todo!("Snapshotting is not implemented.");
            }
            Message::InstallSnapshotResponse { .. } => {
                todo!("Snapshotting is not implemented.");
            }
        }
    }

    async fn delegate(
        &self,
        to_id: usize,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<PutResponse, crate::StoreError> {
        let addr = (self.node_addr)(to_id);
        let mut client = self.connections.connection(addr.clone()).await;
        let action = tonic::Request::new(PutRequest { key, value });
        let response = client.conn.put(action).await.unwrap();
        let response = response.into_inner();

        Ok(response)
    }
}

#[derive(Debug)]
pub struct RpcService {
    pub server: Arc<StoreServer<RpcTransport>>,
}

impl RpcService {
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl Rpc for RpcService {
    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, tonic::Status> {
        let put_data = request.into_inner();
        let server = self.server.clone();

        let results = match server.put(put_data.key, put_data.value).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };

        Ok(Response::new(results))
    }

    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<Empty>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let last_log_index = msg.last_log_index as usize;
        let last_log_term = msg.last_log_term as usize;

        let msg = little_raft::message::Message::VoteRequest {
            from_id,
            term,
            last_log_index,
            last_log_term,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(Empty {}))
    }

    async fn respond_to_vote(
        &self,
        request: Request<VoteResponse>,
    ) -> Result<Response<Empty>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let vote_granted = msg.vote_granted;
        let msg = little_raft::message::Message::VoteResponse {
            from_id,
            term,
            vote_granted,
        };
        let server = self.server.clone();
        server.recv_msg(msg);

        Ok(Response::new(Empty {}))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<Empty>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let prev_log_index = msg.prev_log_index as usize;
        let prev_log_term = msg.prev_log_term as usize;
        let entries: Vec<little_raft::message::LogEntry<StoreCommand>> = msg
            .entries
            .iter()
            .map(|entry| {
                let id = entry.id as usize;
                let transition = StoreCommand {
                    id,
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                };
                let index = entry.index as usize;
                let term = entry.term as usize;
                little_raft::message::LogEntry {
                    transition,
                    index,
                    term,
                }
            })
            .collect();
        let commit_index = msg.commit_index as usize;
        let msg = little_raft::message::Message::AppendEntryRequest {
            from_id,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            commit_index,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(Empty {}))
    }

    async fn respond_to_append_entries(
        &self,
        request: tonic::Request<AppendEntriesResponse>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from_id as usize;
        let term = msg.term as usize;
        let success = msg.success;
        let last_index = msg.last_index as usize;
        let mismatch_index = msg.mismatch_index.map(|idx| idx as usize);
        let msg = little_raft::message::Message::AppendEntryResponse {
            from_id,
            term,
            success,
            last_index,
            mismatch_index,
        };
        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(Empty {}))
    }
}
