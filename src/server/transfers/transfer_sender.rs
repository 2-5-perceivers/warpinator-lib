use std::fs::Metadata;
use std::path::{Path, PathBuf};

use async_walkdir::WalkDir;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::proto::{FileChunk, FileTime};
use crate::remote_manager::RemoteManager;
use crate::server::transfers::{FileType, MovingAverageCalculator};
use crate::types::transfer::{TransferError, TransferState};

const CHUNK_SIZE: usize = 64 * 1024;

pub(crate) async fn send_stream(
    remote_manager: RemoteManager,
    remote_uuid: String,
    transfer_uuid: String,
    source_paths: Vec<PathBuf>,
    tx: Sender<Result<FileChunk, Status>>,
    cancellation_token: CancellationToken,
    #[cfg(feature = "power_manager")] power_manager: std::sync::Arc<
        dyn crate::server::power_manager::PowerManager,
    >,
) {
    #[cfg(feature = "power_manager")]
    let _wake_lock = crate::server::power_manager::WakeLockGuard::new(power_manager);

    let result = send_stream_inner(
        &remote_manager,
        &remote_uuid,
        &transfer_uuid,
        &source_paths,
        &tx,
        &cancellation_token,
    )
    .await;

    let final_state = match result {
        Ok(true) => TransferState::Completed,
        Err(e) if !cancellation_token.is_cancelled() => TransferState::Failed(e),
        _ => TransferState::Canceled,
    };

    remote_manager
        .update_transfer(&remote_uuid, &transfer_uuid, |t| {
            t.state = final_state;
            t.bytes_per_second = 0;
        })
        .await
        .ok();
}

/// Returns Ok(true) if completed, Ok(false) if cancelled, Err on failure
async fn send_stream_inner(
    remote_manager: &RemoteManager,
    remote_uuid: &str,
    transfer_uuid: &str,
    source_paths: &[PathBuf],
    tx: &Sender<Result<FileChunk, Status>>,
    cancellation_token: &CancellationToken,
) -> Result<bool, TransferError> {
    let mut speed = MovingAverageCalculator::new(30);

    for source in source_paths {
        if cancellation_token.is_cancelled() {
            return Ok(false);
        }

        let base = source.parent().ok_or(TransferError::FailedToProcessFiles)?;

        let source_metadata =
            tokio::fs::metadata(source).await.map_err(|e| TransferError::from(e.kind()))?;

        if source_metadata.is_file() {
            send_file(
                source,
                base,
                remote_manager,
                remote_uuid,
                transfer_uuid,
                source_metadata,
                &mut speed,
                tx,
                cancellation_token,
            )
            .await?;
        } else if source_metadata.is_dir() {
            let rel = source.strip_prefix(base).unwrap_or(source);
            let chunk = FileChunk {
                relative_path: rel.to_string_lossy().to_string(),
                file_type: FileType::Directory.into(),
                chunk: vec![].into(),
                file_mode: 0o755, // TODO: read & write actual permissions on Unix systems
                time: None,
                symlink_target: String::new(),
            };
            if tx.send(Ok(chunk)).await.is_err() {
                return Err(TransferError::ConnectionLost);
            }

            // Walk contents
            let mut walker = WalkDir::new(source);
            while let Some(entry) = walker.next().await {
                if cancellation_token.is_cancelled() {
                    return Ok(false);
                }

                let entry = entry.map_err(|_| TransferError::FailedToProcessFiles)?;
                let path = entry.path();
                let metadata = entry.metadata().await.map_err(|e| TransferError::from(e.kind()))?;

                if metadata.is_dir() {
                    let rel = path.strip_prefix(base).unwrap_or(&path);
                    let chunk = FileChunk {
                        relative_path: rel.to_string_lossy().to_string(),
                        file_type: FileType::Directory.into(),
                        chunk: vec![].into(),
                        file_mode: 0o755,
                        time: None,
                        symlink_target: String::new(),
                    };
                    if tx.send(Ok(chunk)).await.is_err() {
                        return Err(TransferError::ConnectionLost);
                    }
                } else if metadata.is_file() {
                    send_file(
                        &path,
                        base,
                        remote_manager,
                        remote_uuid,
                        transfer_uuid,
                        metadata,
                        &mut speed,
                        tx,
                        cancellation_token,
                    )
                    .await?;
                }
                // symlinks skipped
            }
        }
    }

    Ok(true)
}

async fn send_file(
    path: &Path,
    base: &Path,
    remote_manager: &RemoteManager,
    remote_uuid: &str,
    transfer_uuid: &str,
    metadata: Metadata,
    speed: &mut MovingAverageCalculator,
    tx: &Sender<Result<FileChunk, Status>>,
    cancellation_token: &CancellationToken,
) -> Result<(), TransferError> {
    let rel = path.strip_prefix(base).unwrap_or(path);
    let rel_str = rel.to_string_lossy().to_string();

    let file_time = metadata.modified().ok().map(|t| {
        let duration = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        FileTime { mtime: duration.as_secs(), mtime_usec: duration.subsec_millis() * 1000 }
    });

    let mut file = tokio::fs::File::open(path).await.map_err(|e| TransferError::from(e.kind()))?;

    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut first_chunk = true;
    let mut last_chunk_time = std::time::Instant::now();

    loop {
        if cancellation_token.is_cancelled() {
            return Ok(());
        }
        let n = file.read(&mut buffer).await.map_err(|e| TransferError::from(e.kind()))?;
        if n == 0 {
            break; // EOF
        }

        let chunk = FileChunk {
            relative_path: rel_str.clone(),
            file_type: FileType::File.into(),
            chunk: buffer[..n].to_vec().into(),
            file_mode: 0o644,
            time: if first_chunk { file_time.clone() } else { None },
            symlink_target: String::new(),
        };

        first_chunk = false;

        if tx.send(Ok(chunk)).await.is_err() {
            return Err(TransferError::ConnectionLost);
        }

        // Progress
        let elapsed = last_chunk_time.elapsed().as_secs_f64().max(0.001);
        let avg_bps = speed.push(n as u64, elapsed);
        last_chunk_time = std::time::Instant::now();

        remote_manager
            .update_transfer(remote_uuid, transfer_uuid, |t| {
                t.bytes_transferred += n as u64;
                t.bytes_per_second = avg_bps;
            })
            .await
            .ok();
    }

    Ok(())
}
