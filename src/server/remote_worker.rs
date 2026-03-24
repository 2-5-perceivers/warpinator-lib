use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use thiserror::Error;
use tokio::sync::{RwLock, watch};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tracing::instrument;

use crate::config::protocol::ProtocolConfig;
use crate::proto::warp_client::WarpClient;
use crate::proto::{LookupName, OpInfo, StopInfo};
use crate::server::authenticator::{Authenticator, CertUnboxError};
use crate::server::remote_manager::RemoteManager;
use crate::server::transfers::transfer_receiver;
use crate::types::message::{Direction, Message};
use crate::types::remote::{RemoteConnectionError, RemoteState};
use crate::types::transfer::{Transfer, TransferError, TransferKind, TransferState};

#[derive(Error, Debug)]
pub enum ReceiveCertError {
    #[error("Local remote not found")]
    NoRemote,
    #[error("Failed to register with remote: {0}")]
    RegisterServiceError(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to request certificate: {0}")]
    CertificateRequestFailed(String), // status description
    #[error("Group code mismatch")]
    WrongGroupCode,
    #[error("Remote registration service is offline")]
    Offline,
}

#[derive(Error, Debug)]
pub enum ConnectRemoteError {
    #[error("Failed to receive certificate: {0}")]
    CertificateError(ReceiveCertError),
    #[error("Failed to build TLS channel: {0}")]
    TlsError(Box<dyn std::error::Error + Send + Sync>),
    #[error("Ping failed: {0}")]
    PingError(Box<dyn std::error::Error + Send + Sync>),
    #[error("Duplex failed: {0}")]
    DuplexError(Box<dyn std::error::Error + Send + Sync>),
    #[error("Remote worker not found")]
    RemoteWorkerNotFound,
}

type WarpChannel = WarpClient<Channel>;

#[derive(Debug)]
pub struct RemoteWorker {
    uuid: String,
    remote_manager: RemoteManager,
    authenticator: Arc<Authenticator>,
    channel: RwLock<Option<Channel>>,
    client: RwLock<Option<WarpChannel>>,
    cancellation_token: CancellationToken,
    state_tx: watch::Sender<RemoteState>,
    protocol_config: ProtocolConfig,
    server_hostname: String,
    server_ip: IpAddr,
    server_fullname: String,
}

impl RemoteWorker {
    pub(crate) fn new(
        uuid: String,
        remote_manager: RemoteManager,
        authenticator: Arc<Authenticator>,
        root_token: &CancellationToken,
        protocol_config: ProtocolConfig,
        server_hostname: String,
        server_ip: IpAddr,
        server_fullname: String,
    ) -> (Self, watch::Receiver<RemoteState>) {
        let (state_tx, state_rx) = watch::channel(RemoteState::Disconnected);

        let worker = Self {
            uuid,
            remote_manager,
            authenticator,
            channel: RwLock::new(None),
            client: RwLock::new(None),
            cancellation_token: root_token.child_token(),
            state_tx,
            protocol_config,
            server_hostname,
            server_ip,
            server_fullname,
        };

        (worker, state_rx)
    }

    /// Spawns the state loop task. Call once after construction.
    pub(crate) fn spawn_loop(self: Arc<Self>, mut state_rx: watch::Receiver<RemoteState>) {
        async fn wait_then_reconnect(
            remote: &RemoteWorker,
            state_rx: &mut watch::Receiver<RemoteState>,
        ) -> bool {
            tokio::select! {
                _ = remote.cancellation_token.cancelled() => {
                    remote.disconnect().await;
                    false
                },
                _ = sleep(remote.protocol_config.reconnect_interval) => {
                    tracing::debug!(uuid = %remote.uuid, "Attempting reconnect");
                    let _ = remote.connect().await;
                    true
                }
                _ = state_rx.changed() => true,
            }
        }
        tokio::spawn(async move {
            loop {
                let state = state_rx.borrow().clone();

                match state {
                    RemoteState::Connected => {
                        tokio::select! {
                            _ = self.cancellation_token.cancelled() => {
                                self.disconnect().await;
                                break;
                            }
                            _ = sleep(self.protocol_config.ping_interval) => {
                                if let Err(e) = self.ping().await {
                                    tracing::debug!(uuid = %self.uuid, "Ping failed: {}", e);
                                    self.set_state(RemoteState::Disconnected).await;
                                    self.clear_channel().await;
                                }
                            }
                            _ = state_rx.changed() => {}
                        }
                    }
                    RemoteState::Disconnected => {
                        if !wait_then_reconnect(&self, &mut state_rx).await {
                            break;
                        }
                    }
                    RemoteState::Error(ref e) => {
                        if matches!(e, RemoteConnectionError::GroupCodeMismatch) {
                            // Group code mismatch is not retryable, stay in error state until
                            // manual intervention
                            break;
                        }
                        if !wait_then_reconnect(&self, &mut state_rx).await {
                            break;
                        }
                    }
                    _ => {
                        tokio::select! {
                            _ = self.cancellation_token.cancelled() => {
                                self.disconnect().await;
                                break;
                            },
                            _ = state_rx.changed() => {}
                        }
                    }
                }
            }
        });
    }

    #[instrument(skip(self), err(level = "warn"))]
    pub async fn connect(&self) -> Result<(), ConnectRemoteError> {
        self.set_state(RemoteState::Connecting).await;

        let cert_pem = match self.receive_certificate().await {
            Ok(cert) => cert,
            Err(e) => {
                let state = match e {
                    ReceiveCertError::WrongGroupCode => {
                        RemoteState::Error(RemoteConnectionError::GroupCodeMismatch)
                    }
                    ReceiveCertError::Offline => RemoteState::Disconnected,
                    _ => RemoteState::Error(RemoteConnectionError::NoCertificate),
                };

                self.set_state(state).await;
                return Err(ConnectRemoteError::CertificateError(e));
            }
        };

        let channel = match self.build_channel(&cert_pem).await {
            Ok(ch) => ch,
            Err(e) => {
                tracing::warn!(uuid = %self.uuid, "Failed to build TLS channel: {:#?}", e);
                self.set_state(RemoteState::Error(RemoteConnectionError::SslError)).await;
                return Err(ConnectRemoteError::TlsError(e));
            }
        };

        let client = WarpClient::new(channel.clone());
        *self.channel.write().await = Some(channel);
        *self.client.write().await = Some(client);

        if let Err(e) = self.ping().await {
            tracing::warn!(uuid = %self.uuid, "Initial ping failed: {}", e);
            self.clear_channel().await;
            self.set_state(RemoteState::Error(RemoteConnectionError::SslError)).await;
            return Err(ConnectRemoteError::PingError(e));
        }

        self.set_state(RemoteState::AwaitingDuplex).await;
        if let Err(e) = self.wait_for_duplex().await {
            tracing::warn!(uuid = %self.uuid, "Duplex failed: {}", e);
            self.clear_channel().await;
            self.set_state(RemoteState::Error(RemoteConnectionError::DuplexError)).await;
            return Err(ConnectRemoteError::DuplexError(e));
        }

        self.set_state(RemoteState::Connected).await;

        if let Err(e) = self.fetch_machine_info().await {
            tracing::warn!(uuid = %self.uuid, "Failed to get machine info: {}", e);
        }

        if let Err(e) = self.fetch_avatar().await {
            tracing::warn!(uuid = %self.uuid, "Failed to get avatar: {}", e);
        }

        tracing::info!(uuid = %self.uuid, "Connection established");
        Ok(())
    }

    pub(crate) async fn disconnect(&self) {
        self.clear_channel().await;
        self.set_state(RemoteState::Disconnected).await;
    }

    pub fn subscribe_state(&self) -> watch::Receiver<RemoteState> {
        self.state_tx.subscribe()
    }

    async fn receive_certificate(&self) -> Result<Vec<u8>, ReceiveCertError> {
        let remote =
            self.remote_manager.remote(&self.uuid).await.ok_or(ReceiveCertError::NoRemote)?;

        let addr = format!("http://{}:{}", remote.ip, remote.auth_port);
        let reg_channel = Channel::from_shared(addr)
            .map_err(|e| ReceiveCertError::RegisterServiceError(Box::from(e)))?
            .connect_timeout(self.protocol_config.connect_timeout)
            .connect()
            .await
            .map_err(|_| ReceiveCertError::Offline)?;

        let mut reg_client =
            crate::proto::warp_registration_client::WarpRegistrationClient::new(reg_channel);

        let response = reg_client
            .request_certificate(crate::proto::RegRequest {
                hostname: self.server_hostname.to_string(),
                ip: self.server_ip.to_string(),
                ipv6: "".to_string(),
                // TODO: add ipv6 support
            })
            .await
            .map_err(|status| {
                ReceiveCertError::CertificateRequestFailed(status.code().description().into())
            })?
            .into_inner();

        let cleaned_cert = response.locked_cert.replace(&['\n', '\r'][..], "");

        let decoded = STANDARD.decode(cleaned_cert).map_err(|a| {
            ReceiveCertError::CertificateRequestFailed(format!(
                "Failed to decode base64 certificate {}",
                a
            ))
        })?;
        let cert_pem = self.authenticator.unbox_cert(&decoded).map_err(|e| match e {
            CertUnboxError::BoxTooShort => {
                ReceiveCertError::CertificateRequestFailed("Box too short".into())
            }
            CertUnboxError::DecryptionFailed => ReceiveCertError::WrongGroupCode,
        })?;

        self.remote_manager
            .update_remote(&self.uuid, |r| {
                r.cert_pem = Some(cert_pem.clone());
            })
            .await
            .map_err(|_| ReceiveCertError::NoRemote)?;

        Ok(cert_pem)
    }

    async fn build_channel(
        &self,
        cert_pem: &[u8],
    ) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
        let remote = self.remote_manager.remote(&self.uuid).await.ok_or("Remote not found")?;

        let cert = Certificate::from_pem(cert_pem);
        let tls = ClientTlsConfig::new().ca_certificate(cert).domain_name(remote.ip.to_string());

        let addr = format!("https://{}:{}", remote.ip, remote.port);
        let channel = Channel::from_shared(addr)?
            .tls_config(tls)?
            .connect_timeout(self.protocol_config.connect_timeout)
            .connect()
            .await?;

        Ok(channel)
    }

    async fn ping(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.read().await;
        let client = client.as_ref().ok_or("No client")?;
        let mut client = client.clone();

        tokio::time::timeout(
            self.protocol_config.ping_timeout,
            client.ping(LookupName {
                id: self.uuid.clone(),
                readable_name: self.server_hostname.to_string(),
            }),
        )
        .await??;

        Ok(())
    }

    async fn wait_for_duplex(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.read().await;
        let client = client.as_ref().ok_or("No client")?;
        let mut client = client.clone();

        let response = tokio::time::timeout(
            Duration::from_secs(60),
            client.waiting_for_duplex(LookupName {
                id: self.server_fullname.clone(),
                readable_name: self.server_hostname.to_string(),
            }),
        )
        .await??;

        if !response.into_inner().response {
            return Err("Duplex not established".into());
        }

        Ok(())
    }

    async fn fetch_machine_info(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.read().await;
        let client = client.as_ref().ok_or("No client")?;
        let mut client = client.clone();

        let info = client.get_remote_machine_info(LookupName::default()).await?.into_inner();

        self.remote_manager
            .update_remote(&self.uuid, |r| {
                r.display_name = info.display_name.clone();
                r.username = info.user_name.clone();
                // TODO: check for flags
            })
            .await?;

        Ok(())
    }

    async fn fetch_avatar(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.read().await;
        let client = client.as_ref().ok_or("No client")?;
        let mut client = client.clone();

        let mut stream =
            client.get_remote_machine_avatar(LookupName::default()).await?.into_inner();

        let mut bytes = Vec::new();
        while let Some(chunk) = stream.message().await? {
            bytes.extend_from_slice(&chunk.avatar_chunk);
        }

        if !bytes.is_empty() {
            self.remote_manager
                .update_remote(&self.uuid, |r| {
                    r.picture = Some(bytes.clone());
                })
                .await?;
        }

        Ok(())
    }

    pub async fn send_transfer_request(
        &self,
        source_paths: Vec<PathBuf>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.read().await;
        let client = client.as_ref().ok_or("No client")?;
        let mut client = client.clone();

        let transfer_token = self.cancellation_token.child_token();

        let mut transfer =
            Transfer::new_outgoing(self.uuid.clone(), source_paths.clone(), transfer_token).await;

        // Add transfer in initializing state before processing paths, so it appears in
        // UI immediately
        self.remote_manager.add_transfer(&self.uuid, transfer.clone()).await?;

        let processing_result = transfer.process_paths(&source_paths).await;

        match processing_result {
            Ok(_) => {
                client
                    .process_transfer_op_request(transfer.as_proto(self.server_fullname.as_str()))
                    .await?;
                self.remote_manager
                    .update_transfer(&self.uuid, &transfer.uuid, |t| {
                        t.total_bytes = transfer.total_bytes;
                        t.file_count = transfer.file_count;
                        t.entry_names = transfer.entry_names.clone();
                        t.single_name = transfer.single_name.clone();
                        t.single_mime_type = transfer.single_mime_type.clone();
                        t.state = TransferState::WaitingPermission;
                    })
                    .await?;
                Ok(())
            }
            Err(e) => {
                self.remote_manager
                    .update_transfer(&self.uuid, &transfer.uuid, |t| {
                        t.state = TransferState::Failed(TransferError::FailedToProcessFiles);
                    })
                    .await?;
                Err(Box::new(e))
            }
        }
    }

    #[cfg(feature = "messaging")]
    pub async fn send_message(
        &self,
        message: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.read().await;
        let client = client.as_ref().ok_or("No client")?;
        let mut client = client.clone();

        let message = Message::new(self.uuid.clone(), Direction::Sent, message.to_string());

        client.send_text_message(message.as_proto(self.server_fullname.as_str())).await?;

        self.remote_manager.add_message(&self.uuid, message).await?;

        Ok(())
    }

    pub async fn accept_transfer<P: AsRef<Path>>(
        &self,
        transfer_uuid: &str,
        destination: P,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.read().await;
        let client = client.as_ref().ok_or("No client")?;
        let mut client = client.clone();

        let transfer = self
            .remote_manager
            .transfer(self.uuid.as_str(), transfer_uuid)
            .await
            .ok_or("Transfer not found")?;

        let remote_timestamp = match transfer.kind {
            TransferKind::Incoming { destination: _, remote_timestamp } => remote_timestamp,
            TransferKind::Outgoing { .. } => {
                return Err("Cannot accept an outgoing transfer".into());
            }
        };

        let stream = client
            .start_transfer(OpInfo {
                ident: self.server_fullname.clone(),
                timestamp: remote_timestamp,
                use_compression: false,
                readable_name: String::default(),
            })
            .await?
            .into_inner();

        let destination = destination.as_ref().to_path_buf();

        self.remote_manager
            .update_transfer(&self.uuid, transfer_uuid, |t| {
                t.state = TransferState::InProgress;
                t.kind =
                    TransferKind::Incoming { destination: destination.clone(), remote_timestamp };
            })
            .await?;

        tokio::spawn(transfer_receiver::receive_stream(
            self.remote_manager.clone(),
            self.uuid.clone(),
            transfer_uuid.to_string(),
            stream,
            destination,
            self.cancellation_token.child_token(),
        ));

        Ok(())
    }

    /// Stop an in-progress transfer. Don't use on transfers that are not in
    /// progress
    pub async fn stop_transfer(
        &self,
        transfer_uuid: &str,
        error: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.read().await;
        let client = client.as_ref().ok_or("No client")?;
        let mut client = client.clone();

        let transfer = self
            .remote_manager
            .transfer(self.uuid.as_str(), transfer_uuid)
            .await
            .ok_or("Transfer not found")?;

        let timestamp = match transfer.kind {
            TransferKind::Incoming { remote_timestamp, .. } => remote_timestamp,
            TransferKind::Outgoing { cancellation_token, .. } => {
                cancellation_token.cancel();
                transfer.timestamp
            }
        };

        client
            .stop_transfer(StopInfo {
                info: Some(OpInfo {
                    ident: self.server_fullname.clone(),
                    timestamp,
                    readable_name: String::new(),
                    use_compression: false,
                }),
                error,
            })
            .await?;

        self.remote_manager
            .update_transfer(&self.uuid, transfer_uuid, |t| {
                t.state = TransferState::Stopped;
            })
            .await?;

        Ok(())
    }

    /// Reject an incoming transfer or cancel an outgoing one. Use only on
    /// transfers waiting for permission
    pub async fn cancel_transfer(
        &self,
        transfer_uuid: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.read().await;
        let client = client.as_ref().ok_or("No client")?;
        let mut client = client.clone();

        let transfer = self
            .remote_manager
            .transfer(self.uuid.as_str(), transfer_uuid)
            .await
            .ok_or("Transfer not found")?;

        let (new_state, timestamp) = match transfer.kind {
            TransferKind::Incoming { remote_timestamp, .. } => {
                (TransferState::Denied, remote_timestamp)
            }
            TransferKind::Outgoing { cancellation_token, .. } => {
                cancellation_token.cancel();
                (TransferState::Canceled, transfer.timestamp)
            }
        };

        client
            .cancel_transfer_op_request(OpInfo {
                ident: self.server_fullname.clone(),
                timestamp,
                readable_name: String::new(),
                use_compression: false,
            })
            .await?;

        self.remote_manager
            .update_transfer(&self.uuid, transfer_uuid, |t| {
                t.state = new_state;
            })
            .await?;

        Ok(())
    }

    async fn set_state(&self, state: RemoteState) {
        let _ = self.state_tx.send(state.clone());
        let _ = self
            .remote_manager
            .update_remote(&self.uuid, |r| {
                r.state = state;
            })
            .await;
    }

    async fn clear_channel(&self) {
        *self.channel.write().await = None;
        *self.client.write().await = None;
    }
}
