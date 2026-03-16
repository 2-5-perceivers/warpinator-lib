use crate::proto::TransferOpRequest;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Clone, Debug)]
pub enum TransferError {
    #[error("Connection was lost")]
    ConnectionLost,
    #[error("Storage filled up")]
    StorageFull,
}

#[derive(Clone, Debug)]
pub enum TransferState {
    /// New outgoing transfer
    Initializing,
    /// Waiting for the other party to accept the transfer
    WaitingPermission,
    /// Transfer is in progress
    InProgress,
    /// Transfer is paused
    Paused,
    /// Transfer is completed
    Completed,
    /// Transfer is canceled
    Canceled,
    /// Transfer was denied by the other party
    Denied,
    /// Transfer failed due to an error
    Failed(TransferError),
}

#[derive(Clone, Debug)]
pub struct Transfer {
    /// Unique identifier for this transfer
    pub uuid: String,
    /// Unique identifier of the parent remote
    pub remote_uuid: String,

    /// Current state of the transfer
    pub state: TransferState,
    /// Timestamp of the time when the transfer was created(sent/received) in milliseconds
    pub timestamp: u64,
    /// The timestamp of the transfer on the remote side. This is used as id for the transfer in the protocol. Ironically, this might not be a timestamp
    pub(crate) remote_timestamp: u64,

    /// Total size of the transfer in bytes
    pub total_bytes: u64,
    /// Number of bytes transferred so far
    pub bytes_transferred: u64,
    /// Current transfer speed in bytes per second. Moving average
    pub bytes_per_second: u64,

    /// Number of total files in the transfer
    pub file_count: u64,
    /// Names of the top dir entries in the transfer
    pub entry_names: Vec<String>,
    /// Utilized only if the transfer contains a single file. Name of the file being transferred
    pub single_name: Option<String>,
    /// Utilized only if the transfer contains a single file. MIME type of the file being transferred
    pub single_mime_type: Option<String>,

    /// Kind of transfer - incoming or outgoing. Contains additional data relevant to the kind
    pub kind: TransferKind,
}

#[derive(Clone, Debug)]
pub enum TransferKind {
    Outgoing { source_paths: Vec<PathBuf> },
    Incoming { destination: PathBuf },
}

impl From<TransferOpRequest> for Transfer {
    fn from(value: TransferOpRequest) -> Self {
        Transfer {
            uuid: uuid::Uuid::new_v4().to_string(),
            remote_uuid: value
                .info
                .as_ref()
                .expect("TransferOpRequest must have info")
                .ident
                .clone(),
            state: TransferState::WaitingPermission,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            remote_timestamp: value
                .info
                .expect("TransferOpRequest must have info")
                .timestamp,
            total_bytes: value.size,
            bytes_transferred: 0,
            bytes_per_second: 0,
            file_count: value.count,
            entry_names: value.top_dir_basenames,
            single_name: if value.count == 1 && !value.name_if_single.is_empty() {
                value.name_if_single.into()
            } else {
                None
            },
            single_mime_type: if value.count == 1 && !value.mime_if_single.is_empty() {
                value.mime_if_single.into()
            } else {
                None
            },
            kind: TransferKind::Incoming {
                destination: PathBuf::from("/tmp"), // default destination, should be updated when transfer is accepted
            },
        }
    }
}
