pub(crate) mod proto {
    tonic::include_proto!("warpinator");
}

pub mod config;
pub(crate) mod grpc;
pub(crate) mod server;
pub mod types;

pub use server::{WarpinatorServer, remote_manager};
