use crate::proto::FileChunk;
use crate::remote_manager::RemoteManager;
use crate::types::transfer::{TransferError, TransferState};
use std::collections::VecDeque;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tonic::Streaming;
use tracing::instrument;

#[allow(unused)]
mod file_type {
    pub const FILE: u8 = 1;
    pub const DIRECTORY: u8 = 2;
    pub const SYMLINK: u8 = 3;
}

struct MovingAverage {
    samples: VecDeque<u64>,
    window: usize,
}

impl MovingAverage {
    fn new(window: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(window),
            window,
        }
    }

    fn push(&mut self, value: u64) -> u64 {
        if self.samples.len() == self.window {
            self.samples.pop_front();
        }
        self.samples.push_back(value);
        self.samples.iter().sum::<u64>() / self.samples.len() as u64
    }
}

struct ReceiveState {
    current_path: Option<String>,
    current_file: Option<tokio::fs::File>,
    speed: MovingAverage,
    last_chunk_time: std::time::Instant,
}

impl ReceiveState {
    fn new() -> Self {
        Self {
            current_path: None,
            current_file: None,
            speed: MovingAverage::new(30),
            last_chunk_time: std::time::Instant::now(),
        }
    }

    async fn close_current_file(&mut self) {
        if let Some(mut file) = self.current_file.take() {
            let _ = file.flush().await;
        }
        self.current_path = None;
    }
}
fn sanitize_path_component(name: &str) -> String {
    name.replace(['\\', '<', '>', '*', '|', '?', ':', '"'], "_")
}

/// Returns Ok(true) it was completed, Ok(false) if it was canceled, and Err(TransferError) it failed
#[instrument(
    skip(remote_manager, stream, cancellation_token),
    level = "debug",
    err(level = "warn")
)]
async fn receive_stream_inner(
    remote_manager: &RemoteManager,
    remote_uuid: &str,
    transfer_uuid: &str,
    stream: &mut Streaming<FileChunk>,
    destination: &PathBuf,
    cancellation_token: &CancellationToken,
) -> Result<bool, TransferError> {
    let mut state = ReceiveState::new();

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Transfer cancelled via token");
                state.close_current_file().await;
                return Ok(false);
            }
            msg_result = stream.message() => {
                match msg_result {
                    Ok(Some(chunk)) => {

        if chunk.file_type == file_type::SYMLINK as i32 {
            continue;
        }

        let sanitized = sanitize_path_component(&chunk.relative_path);
        let target_path = destination.join(&sanitized);

        // Check that the target path is within the destination directory
        if sanitized.contains("..") && !target_path.starts_with(&destination) {
            return Err(TransferError::UnsafePath);
        }

        if state.current_path.as_deref() != Some(&sanitized) {
            state.close_current_file().await;
            state.current_path = Some(sanitized.clone());

            if chunk.file_type == file_type::DIRECTORY as i32 {
                tokio::fs::create_dir_all(&target_path).await.ok();
                continue;
            }

            if let Some(parent) = target_path.parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }

            match tokio::fs::File::create(&target_path).await {
                Ok(file) => state.current_file = Some(file),
                Err(e) => return Err(e.kind().into()),
            }
        }

        // Write chunk
        if let Some(file) = state.current_file.as_mut() {
            let data = chunk.chunk;
            let chunk_len = data.len() as u64;

            if let Err(e) = file.write_all(&data).await {
                return Err(e.kind().into());
            }

            // Progress
            let elapsed = state.last_chunk_time.elapsed().as_secs_f64().max(0.001);
            let bps = (chunk_len as f64 / elapsed) as u64;
            let avg_bps = state.speed.push(bps);
            state.last_chunk_time = std::time::Instant::now();

            remote_manager
                .update_transfer(&remote_uuid, &transfer_uuid, |t| {
                    t.bytes_transferred += chunk_len;
                    t.bytes_per_second = avg_bps;
                })
                .await
                .ok();
        }

                    }
                    Ok(None) => {
                        // Stream finished successfully
                        break;
                    }
                    Err(_) => {
                        state.close_current_file().await;
                        return Err(TransferError::ConnectionLost);
                    }
                }
            }
        }
    }

    // Done
    state.close_current_file().await;
    Ok(true)
}

pub(crate) async fn receive_stream(
    remote_manager: RemoteManager,
    remote_uuid: String,
    transfer_uuid: String,
    mut stream: Streaming<FileChunk>,
    destination: PathBuf,
    cancellation_token: CancellationToken,
) {
    let result = receive_stream_inner(
        &remote_manager,
        &remote_uuid,
        &transfer_uuid,
        &mut stream,
        &destination,
        &cancellation_token,
    )
    .await;

    let final_state = match result {
        Ok(true) => TransferState::Completed,
        Ok(false) => TransferState::Canceled,
        Err(e) => TransferState::Failed(e),
    };

    remote_manager
        .update_transfer(&remote_uuid, &transfer_uuid, |t| {
            t.state = final_state;
            t.bytes_per_second = 0;
        })
        .await
        .ok();
}
