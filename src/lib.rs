pub(crate) mod proto {
    tonic::include_proto!("warpinator");
}

pub mod config;
pub(crate) mod grpc;
pub(crate) mod server;
pub mod types;

pub use server::remote_worker::ConnectRemoteError;
pub use server::{WarpinatorBuildError, WarpinatorServeError, WarpinatorServer, remote_manager};
