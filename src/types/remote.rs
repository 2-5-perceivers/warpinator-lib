use std::net::IpAddr;

use thiserror::Error;

#[cfg(feature = "messaging")]
use crate::types::message::Message;
use crate::types::transfer::Transfer;

#[derive(Error, Clone, Debug, PartialEq, Eq)]
pub enum RemoteConnectionError {
    #[error("SSL connection failed")]
    SslError,
    #[error("Group code mismatch")]
    GroupCodeMismatch,
    #[error("No certificate")]
    NoCertificate,
    #[error("Duplex connection failed")]
    DuplexError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoteState {
    Error(RemoteConnectionError),
    Disconnected,
    Connecting,
    AwaitingDuplex,
    Connected,
}

#[derive(Clone, Debug)]
pub struct Remote {
    pub uuid: String,
    pub ip: IpAddr,
    pub port: u16,
    pub auth_port: u16,
    pub service_name: String,

    pub display_name: String,
    pub username: String,
    pub hostname: String,
    pub picture: Option<Vec<u8>>,

    pub state: RemoteState,

    #[cfg(feature = "messaging")]
    pub messages: Vec<Message>,
    pub transfers: Vec<Transfer>,

    /// Whether the remote's service is static (i.e. registered) or dynamic
    /// (i.e. discovered on the network)
    pub service_static: bool,
    /// Whether the remote's mdns service is currently available
    pub service_available: bool,
    /// Unboxed PEM certificate for the remote
    pub cert_pem: Option<Vec<u8>>,
}

impl Remote {
    pub fn new(
        uuid: String,
        ip: IpAddr,
        port: u16,
        auth_port: u16,
        service_name: String,
        hostname: String,
    ) -> Self {
        Self {
            uuid,
            ip,
            port,
            auth_port,
            service_name,
            display_name: "".to_string(),
            username: "".to_string(),
            hostname,
            picture: None,
            state: RemoteState::Disconnected,
            #[cfg(feature = "messaging")]
            messages: vec![],
            transfers: vec![],
            service_static: false,
            service_available: false,
            cert_pem: None,
        }
    }
}
