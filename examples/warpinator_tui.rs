mod tui;

use anyhow::Result;
use ratatui::crossterm::event::KeyCode;
use ratatui::crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event as CEvent},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc as std_mpsc,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};
use tui::app::InputMode;
use tui::app::{App, AppEvent};
use warpinator_lib::WarpinatorServer;
use warpinator_lib::config::user::UserConfig;
use warpinator_lib::remote_manager::RemoteManager;

#[derive(Clone)]
struct TuiLogWriter {
    tx: mpsc::Sender<AppEvent>,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TuiLogWriter {
    type Writer = TuiLogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl io::Write for TuiLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Ok(s) = std::str::from_utf8(buf) {
            let line = s.trim_end_matches('\n').to_string();
            if !line.is_empty() {
                let _ = self.tx.try_send(AppEvent::Log(line));
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<AppEvent>(512);

    let group_code =
        env::var("WARPINATOR_GROUP_CODE").unwrap_or_else(|_| "Warpinator-TUI".to_string());
    let display_name =
        env::var("WARPINATOR_DISPLAY_NAME").unwrap_or_else(|_| "Warpinator RS".to_string());
    let picture = env::var("WARPINATOR_PICTURE")
        .ok()
        .and_then(|path| fs::read(path).ok());
    let username = env::var("USER")
        .or_else(|_| env::var("USERNAME"))
        .or_else(|_| env::var("WARPINATOR_USERNAME"))
        .unwrap_or_else(|_| "warpinator-rs".to_string());
    let hostname = hostname::get()
        .ok()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|| "warpinator".to_string());
    let mut hasher = Sha256::new();
    hasher.update(hostname.as_bytes());
    hasher.update(username.as_bytes());
    hasher.update(group_code.as_bytes());
    hasher.update(display_name.as_bytes());
    let hash = hasher.finalize();
    let service_id = format!(
        "WARPINATOR-{:X}",
        u64::from_be_bytes([
            hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7]
        ])
    );

    let mut user_config_builder = UserConfig::builder()
        .default_bind_addr_v4()
        .default_bind_addr_v6()
        .hostname(&hostname)
        .username(&username)
        .display_name(&display_name)
        .group_code(&group_code);
    if let Some(pic) = picture {
        user_config_builder = user_config_builder.picture(&pic);
    }
    let user_config = user_config_builder.build();

    let server = WarpinatorServer::builder()
        .user_config(user_config)
        .service_name(&service_id)
        .build()
        .expect("failed to build server");

    let remote_manager = server.remotes.clone();
    let mut warp_events = server.remotes.subscribe();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        server
            .serve_with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("server error");
    });

    let tx_term = tx.clone();
    let (term_shutdown_tx, term_shutdown_rx) = std_mpsc::channel::<()>();
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let term_handle = tokio::task::spawn_blocking(move || {
        while running_clone.load(Ordering::Relaxed) {
            if let Ok(_) = term_shutdown_rx.try_recv() {
                break;
            }
            if let Ok(ev) = ratatui::crossterm::event::read() {
                if tx_term.blocking_send(AppEvent::Terminal(ev)).is_err() {
                    break;
                }
            }
        }
    });

    let tx_lib = tx.clone();
    let rm_for_events = remote_manager.clone();
    tokio::spawn(async move {
        while let Ok(ev) = warp_events.recv().await {
            if let Some(app_ev) = AppEvent::hydrate(ev, &rm_for_events).await {
                if tx_lib.send(app_ev).await.is_err() {
                    break;
                }
            }
        }
    });

    let tui_writer = TuiLogWriter { tx: tx.clone() };
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(tui_writer)
                .with_ansi(false)
                .with_span_events(FmtSpan::CLOSE),
        )
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .init();

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();
    app.log("warpinator-tui started");

    let result = run(&mut terminal, &mut app, &mut rx, remote_manager).await;

    running.store(false, Ordering::Relaxed);
    let _ = term_shutdown_tx.send(());
    let _ = term_handle.await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    let _ = shutdown_tx.send(());

    result
}

async fn run(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    rx: &mut mpsc::Receiver<AppEvent>,
    remote_manager: RemoteManager,
) -> Result<()> {
    let mut draw_tick = tokio::time::interval(Duration::from_millis(33)); // ~30fps

    loop {
        tokio::select! {
            _ = draw_tick.tick() => {
                terminal.draw(|f| tui::ui::draw(f, app))?;
            }
            ev = rx.recv() => {
                match ev {
                    Some(AppEvent::Terminal(CEvent::Key(key))) => {
                        #[cfg(feature = "messaging")]
                        {
                            if key.code == KeyCode::Enter {
                                if let Some(InputMode::Message) = app.input_mode {
                                    if let Some(remote) = app.current_remote() {
                                        let msg = app.input_buf.clone();
                                        let remote_uuid = remote.uuid.clone();
                                        let rm = remote_manager.clone();
                                        tokio::spawn(async move {
                                            if let Some(worker) = rm.get_worker(&remote_uuid).await {
                                                let _ = worker.send_message(&msg).await;
                                            }
                                        });
                                    }
                                }
                            }
                        }

                        // Handle file path send (split by ';' into multiple paths)
                        if key.code == KeyCode::Enter {
                            if let Some(InputMode::FilePath) = app.input_mode {
                                if let Some(remote) = app.current_remote() {
                                    let value = app.input_buf.clone();
                                    let remote_uuid = remote.uuid.clone();
                                    let rm = remote_manager.clone();
                                    tokio::spawn(async move {
                                        if let Some(worker) = rm.get_worker(&remote_uuid).await {
                                            let paths: Vec<std::path::PathBuf> = value
                                                .split(';')
                                                .map(|s| s.trim())
                                                .filter(|s| !s.is_empty())
                                                .map(|s| std::path::PathBuf::from(s))
                                                .collect();
                                            if !paths.is_empty() {
                                                let _ = worker.send_transfer_request(paths).await;
                                            }
                                        }
                                    });
                                }
                            }
                        }

                        if app.handle_key(key).is_quit() {
                            break;
                        }
                    }
                    Some(ev) => app.handle_event(ev),
                    None => break,
                }
            }
        }
    }

    Ok(())
}
