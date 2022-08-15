use anyhow::Result;
use std::sync::Arc;
use tonic::transport::Server;

pub mod error;
mod kvstore;
pub mod rpc;
pub mod server;

pub use error::StoreError;
pub use server::StoreCommand;
pub use server::StoreServer;
pub use server::StoreTransport;

use structopt::StructOpt;
use rpc::{RpcService, RpcTransport};
pub use rpc::proto::rpc_server::RpcServer;

#[derive(StructOpt, Debug)]
#[structopt(name = "gouged")]
struct Opt {
    /// The ID of this server.
    #[structopt(short, long)]
    id: usize,
    /// The IDs of peers.
    #[structopt(short, long, required = false)]
    peers: Vec<usize>,
}

fn node_data(id: usize) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

fn node_rpc_addr(id: usize) -> String {
    let (host, port) = node_data(id);
    format!("http://{}:{}", host, port)
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    let (host, port) = node_data(opt.id);
    let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
    let transport = RpcTransport::new(Box::new(node_rpc_addr));
    let server = StoreServer::start(opt.id, opt.peers, transport)?;
    let server = Arc::new(server);
    let f = {
        let server = server.clone();
        tokio::task::spawn_blocking(move || {
            server.run();
        })
    };
    let rpc = RpcService::new(server);
    let g = tokio::task::spawn(async move {
        println!("RPC listening to {} ...", rpc_listen_addr);
        let ret = Server::builder()
            .add_service(RpcServer::new(rpc))
            .serve(rpc_listen_addr)
            .await;
        ret
    });
    let results = tokio::try_join!(f, g)?;
    results.1?;
    Ok(())
}
