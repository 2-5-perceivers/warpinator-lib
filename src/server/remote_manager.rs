use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{RwLock, broadcast};
use tokio_util::sync::CancellationToken;

use crate::config::protocol::ProtocolConfig;
use crate::server::authenticator::Authenticator;
use crate::server::remote_worker::RemoteWorker;
#[cfg(feature = "messaging")]
use crate::types::message::Message;
use crate::types::remote::Remote;
use crate::types::transfer::Transfer;

#[non_exhaustive]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum WarpEvent {
    RemoteAdded(String),             // uuid
    RemoteUpdated(String),           // uuid
    TransferAdded(String, String),   // remote_uuid, transfer_uuid
    TransferUpdated(String, String), // remote_uuid, transfer_uuid
    TransferRemoved(String, String), // remote_uuid, transfer_uuid
    #[cfg(feature = "messaging")]
    MessageAdded(String, String), // remote_uuid, message_uuid
    #[cfg(feature = "messaging")]
    MessageRemoved(String, String), // remote_uuid, message_uuid
}

#[derive(Error, Debug)]
pub enum UpdateError {
    #[error("Resource not found")]
    NotFound,
}

#[derive(Debug)]
pub(crate) struct RemoteManagerInner {
    remotes: RwLock<HashMap<String, Remote>>,
    workers: RwLock<HashMap<String, Arc<RemoteWorker>>>,
    event_tx: broadcast::Sender<WarpEvent>,
    root_token: CancellationToken,
    authenticator: Arc<Authenticator>,
    protocol_config: ProtocolConfig,
    server_hostname: String,
    server_ip: IpAddr,
    server_fullname: String,
}

#[derive(Clone, Debug)]
pub struct RemoteManager {
    inner: Arc<RemoteManagerInner>,
}

impl RemoteManager {
    pub(crate) fn new(
        cancellation_token: CancellationToken,
        authenticator: Arc<Authenticator>,
        protocol_config: ProtocolConfig,
        server_hostname: String,
        server_ip: IpAddr,
        server_fullname: String,
    ) -> Self {
        let (tx, _) = broadcast::channel(64);
        let inner = Arc::new(RemoteManagerInner {
            remotes: RwLock::new(HashMap::new()),
            workers: RwLock::new(HashMap::new()),
            event_tx: tx,
            root_token: cancellation_token,
            authenticator,
            protocol_config,
            server_hostname,
            server_ip,
            server_fullname,
        });
        Self { inner }
    }

    pub(crate) async fn add_remote(&self, remote: Remote) -> Arc<RemoteWorker> {
        let uuid = remote.uuid.clone();

        let (worker, state_rx) = RemoteWorker::new(
            uuid.clone(),
            self.clone(),
            Arc::clone(&self.inner.authenticator),
            &self.inner.root_token,
            self.inner.protocol_config.clone(),
            self.inner.server_hostname.clone(),
            self.inner.server_ip,
            self.inner.server_fullname.clone(),
        );

        let worker = Arc::new(worker);
        self.inner.workers.write().await.insert(uuid.clone(), worker.clone());
        self.inner.remotes.write().await.insert(uuid.clone(), remote);

        worker.clone().spawn_loop(state_rx);

        let _ = self.inner.event_tx.send(WarpEvent::RemoteAdded(uuid));

        worker
    }

    pub(crate) async fn update_remote(
        &self,
        uuid: &str,
        f: impl FnOnce(&mut Remote),
    ) -> Result<(), UpdateError> {
        if let Some(remote) = self.inner.remotes.write().await.get_mut(uuid) {
            f(remote);
            let _ = self.inner.event_tx.send(WarpEvent::RemoteUpdated(uuid.to_string()));
            Ok(())
        } else {
            Err(UpdateError::NotFound)
        }
    }

    pub(crate) async fn add_transfer(
        &self,
        remote_uuid: &str,
        transfer: Transfer,
    ) -> Result<(), UpdateError> {
        let transfer_uuid = transfer.uuid.clone();
        if let Some(remote) = self.inner.remotes.write().await.get_mut(remote_uuid) {
            remote.transfers.push(transfer);
            let _ = self
                .inner
                .event_tx
                .send(WarpEvent::TransferAdded(remote_uuid.to_string(), transfer_uuid));
            Ok(())
        } else {
            Err(UpdateError::NotFound)
        }
    }

    pub(crate) async fn update_transfer(
        &self,
        remote_uuid: &str,
        transfer_uuid: &str,
        f: impl FnOnce(&mut Transfer),
    ) -> Result<(), UpdateError> {
        if let Some(remote) = self.inner.remotes.write().await.get_mut(remote_uuid)
            && let Some(transfer) = remote.transfers.iter_mut().find(|t| t.uuid == transfer_uuid)
        {
            f(transfer);
            let _ = self.inner.event_tx.send(WarpEvent::TransferUpdated(
                remote_uuid.to_string(),
                transfer_uuid.to_string(),
            ));
            return Ok(());
        }
        Err(UpdateError::NotFound)
    }

    pub async fn remove_transfer(
        &self,
        remote_uuid: &str,
        transfer_uuid: &str,
    ) -> Result<(), UpdateError> {
        if let Some(remote) = self.inner.remotes.write().await.get_mut(remote_uuid)
            && let Some(pos) = remote.transfers.iter().position(|t| t.uuid == transfer_uuid)
        {
            remote.transfers.remove(pos);
            let _ = self.inner.event_tx.send(WarpEvent::TransferRemoved(
                remote_uuid.to_string(),
                transfer_uuid.to_string(),
            ));
            return Ok(());
        }
        Err(UpdateError::NotFound)
    }

    #[cfg(feature = "messaging")]
    pub(crate) async fn add_message(
        &self,
        remote_uuid: &str,
        message: Message,
    ) -> Result<(), UpdateError> {
        let message_uuid = message.uuid.clone();
        if let Some(remote) = self.inner.remotes.write().await.get_mut(remote_uuid) {
            remote.messages.push(message);
            let _ = self
                .inner
                .event_tx
                .send(WarpEvent::MessageAdded(remote_uuid.to_string(), message_uuid));
            Ok(())
        } else {
            Err(UpdateError::NotFound)
        }
    }

    #[cfg(feature = "messaging")]
    pub async fn remove_message(
        &self,
        remote_uuid: &str,
        message_uuid: &str,
    ) -> Result<(), UpdateError> {
        if let Some(remote) = self.inner.remotes.write().await.get_mut(remote_uuid)
            && let Some(pos) = remote.messages.iter().position(|m| m.uuid == message_uuid)
        {
            remote.messages.remove(pos);
            let _ = self
                .inner
                .event_tx
                .send(WarpEvent::MessageRemoved(remote_uuid.to_string(), message_uuid.to_string()));
            return Ok(());
        }
        Err(UpdateError::NotFound)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WarpEvent> {
        self.inner.event_tx.subscribe()
    }

    pub async fn get_worker(&self, uuid: &str) -> Option<Arc<RemoteWorker>> {
        self.inner.workers.read().await.get(uuid).cloned()
    }

    pub async fn remote(&self, uuid: &str) -> Option<Remote> {
        self.inner.remotes.read().await.get(uuid).cloned()
    }

    pub async fn remotes(&self) -> Vec<Remote> {
        self.inner.remotes.read().await.values().cloned().collect()
    }

    pub async fn transfer(&self, remote_uuid: &str, transfer_uuid: &str) -> Option<Transfer> {
        self.inner
            .remotes
            .read()
            .await
            .get(remote_uuid)
            .and_then(|r| r.transfers.iter().find(|t| t.uuid == transfer_uuid).cloned())
    }

    pub async fn transfer_by_timestamp(
        &self,
        remote_uuid: &str,
        transfer_timestamp: u64,
    ) -> Option<Transfer> {
        self.inner
            .remotes
            .read()
            .await
            .get(remote_uuid)
            .and_then(|r| r.transfers.iter().find(|t| t.timestamp == transfer_timestamp).cloned())
    }

    pub async fn transfers(&self, remote_uuid: &str) -> Option<Vec<Transfer>> {
        self.inner.remotes.read().await.get(remote_uuid).map(|r| r.transfers.clone())
    }

    #[cfg(feature = "messaging")]
    pub async fn message(&self, remote_uuid: &str, message_uuid: &str) -> Option<Message> {
        self.inner
            .remotes
            .read()
            .await
            .get(remote_uuid)
            .and_then(|r| r.messages.iter().find(|m| m.uuid == message_uuid).cloned())
    }

    #[cfg(feature = "messaging")]
    pub async fn messages(&self, remote_uuid: &str) -> Option<Vec<Message>> {
        self.inner.remotes.read().await.get(remote_uuid).map(|r| r.messages.clone())
    }
}
