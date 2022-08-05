mod kvstore;
pub mod rpc;
pub mod server;
pub mod error;

pub use server::StoreCommand;
pub use server::StoreServer;
pub use server::StoreTransport;
pub use error::StoreError;

fn main() {
    println!("Hello, world!");
}
