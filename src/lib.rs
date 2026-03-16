pub(crate) mod proto {
    tonic::include_proto!("warpinator");
}

mod server;

pub use server::UserConfig;
pub use server::WarpinatorServer;
#[cfg(feature = "messaging")]
pub use server::message;
pub use server::remote;
pub use server::remote_manager;
pub use server::transfer;
