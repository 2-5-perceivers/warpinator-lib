use ratatui::crossterm::event::{KeyCode, KeyEvent};
use std::collections::{HashMap, VecDeque};
use warpinator_lib::remote::Message;
use warpinator_lib::{
    remote_manager::{RemoteManager, WarpEvent},
    {remote::Remote, transfer::Transfer},
};

pub enum AppEvent {
    Terminal(ratatui::crossterm::event::Event),
    RemoteAdded(Remote),
    RemoteUpdated(Remote),
    TransferAdded(String, Transfer),   // remote_uuid, transfer
    TransferUpdated(String, Transfer), // remote_uuid, transfer
    Log(String),
    #[cfg(feature = "messaging")]
    MessageAdded(String, warpinator_lib::remote::Message), // remote_uuid, message
}

impl AppEvent {
    pub async fn hydrate(ev: WarpEvent, rm: &RemoteManager) -> Option<Self> {
        match ev {
            WarpEvent::RemoteAdded(uuid) => Some(AppEvent::RemoteAdded(rm.remote(&uuid).await?)),
            WarpEvent::RemoteUpdated(uuid) => {
                Some(AppEvent::RemoteUpdated(rm.remote(&uuid).await?))
            }
            WarpEvent::TransferAdded(remote_uuid, transfer_uuid) => {
                let transfer = rm.transfer(&remote_uuid, &transfer_uuid).await?;
                Some(AppEvent::TransferAdded(remote_uuid, transfer))
            }
            WarpEvent::TransferUpdated(remote_uuid, transfer_uuid) => {
                let transfer = rm.transfer(&remote_uuid, &transfer_uuid).await?;
                Some(AppEvent::TransferUpdated(remote_uuid, transfer))
            }
            #[cfg(feature = "messaging")]
            WarpEvent::MessageAdded(remote_uuid, message_uuid) => {
                let remote = rm.remote(&remote_uuid).await?;
                let message = remote
                    .messages
                    .iter()
                    .find(|m| m.uuid == message_uuid)?
                    .clone();
                Some(AppEvent::MessageAdded(remote_uuid, message))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Focus {
    Remotes,
    Transfers,
    Messages,
}

#[derive(Debug, Clone)]
pub enum InputMode {
    FilePath,
    Message,
}

pub enum Action {
    None,
    Quit,
}

impl Action {
    pub fn is_quit(&self) -> bool {
        matches!(self, Action::Quit)
    }
}

pub struct App {
    pub remotes: Vec<Remote>,
    pub selected_remote: usize,
    pub transfers: HashMap<String, Vec<Transfer>>,
    pub selected_transfer: usize,
    pub focus: Focus,
    pub input_mode: Option<InputMode>,
    pub input_buf: String,
    pub log: VecDeque<String>,
}

impl App {
    pub fn new() -> Self {
        Self {
            remotes: Vec::new(),
            selected_remote: 0,
            transfers: HashMap::new(),
            selected_transfer: 0,
            focus: Focus::Remotes,
            input_mode: None,
            input_buf: String::new(),
            log: VecDeque::with_capacity(200),
        }
    }

    pub fn log(&mut self, msg: impl Into<String>) {
        let msg = msg.into();
        if self.log.len() == 200 {
            self.log.pop_front();
        }
        self.log.push_back(msg);
    }

    pub fn current_remote(&self) -> Option<&Remote> {
        self.remotes.get(self.selected_remote)
    }

    pub fn current_transfers(&self) -> &[Transfer] {
        self.current_remote()
            .and_then(|r| self.transfers.get(&r.uuid))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn current_messages(&self) -> &[Message] {
        self.current_remote()
            .map(|r| r.messages.as_slice())
            .unwrap_or(&[])
    }

    pub fn handle_event(&mut self, ev: AppEvent) {
        match ev {
            AppEvent::RemoteAdded(remote) => {
                self.transfers.entry(remote.uuid.clone()).or_default();
                self.remotes.push(remote);
            }
            AppEvent::RemoteUpdated(remote) => {
                if let Some(r) = self.remotes.iter_mut().find(|r| r.uuid == remote.uuid) {
                    *r = remote;
                }
            }
            AppEvent::TransferAdded(remote_uuid, transfer) => {
                self.transfers
                    .entry(remote_uuid)
                    .or_default()
                    .push(transfer);
            }
            AppEvent::TransferUpdated(remote_uuid, transfer) => {
                if let Some(transfers) = self.transfers.get_mut(&remote_uuid) {
                    if let Some(t) = transfers.iter_mut().find(|t| t.uuid == transfer.uuid) {
                        *t = transfer;
                    }
                }
            }
            #[cfg(feature = "messaging")]
            AppEvent::MessageAdded(remote_uuid, message) => {
                if let Some(remote) = self.remotes.iter_mut().find(|r| r.uuid == remote_uuid) {
                    remote.messages.push(message);
                }
            }
            AppEvent::Log(line) => {
                self.log(line);
            }
            AppEvent::Terminal(_) => {}
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Action {
        if self.input_mode.is_some() {
            return self.handle_input_key(key);
        }

        match key.code {
            KeyCode::Char('q') => return Action::Quit,
            KeyCode::Tab => {
                self.focus = match self.focus {
                    Focus::Remotes => Focus::Transfers,
                    Focus::Transfers => Focus::Messages,
                    Focus::Messages => Focus::Remotes,
                };
            }
            KeyCode::Char('j') | KeyCode::Down => self.move_down(),
            KeyCode::Char('k') | KeyCode::Up => self.move_up(),
            KeyCode::Char('s') => {
                if self.current_remote().is_some() {
                    self.input_mode = Some(InputMode::FilePath);
                    self.input_buf.clear();
                }
            }
            KeyCode::Char('m') => {
                if self.current_remote().is_some() {
                    self.input_mode = Some(InputMode::Message);
                    self.input_buf.clear();
                }
            }
            KeyCode::Char('a') => {
                if let Some(t) = self.current_transfers().get(self.selected_transfer) {
                    // TODO: rm.get_worker(remote_uuid).accept_transfer(&t.uuid)
                }
            }
            KeyCode::Char('r') => {
                if let Some(t) = self.current_transfers().get(self.selected_transfer) {
                    // TODO: rm.get_worker(remote_uuid).reject_transfer(&t.uuid)
                }
            }
            KeyCode::Char('p') => {
                if let Some(t) = self.current_transfers().get(self.selected_transfer) {
                    // TODO: control channel on TransferKind::Outgoing
                }
            }
            KeyCode::Char('x') => {
                if let Some(t) = self.current_transfers().get(self.selected_transfer) {
                    // TODO: control channel on TransferKind::Outgoing
                }
            }
            _ => {}
        }

        Action::None
    }

    fn handle_input_key(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = None;
                self.input_buf.clear();
            }
            KeyCode::Enter => {
                let value = std::mem::take(&mut self.input_buf);
                match self.input_mode.take() {
                    Some(InputMode::FilePath) => {
                        if let Some(remote) = self.current_remote() {
                            self.log(format!("send '{}' -> {}", value, remote.uuid));
                            // TODO: rm.get_worker(&remote.uuid).send_file(PathBuf::from(value))
                        }
                    }
                    Some(InputMode::Message) => {
                        if let Some(remote) = self.current_remote() {
                            self.log(format!("msg '{}' -> {}", value, remote.uuid));
                        }
                    }
                    None => {}
                }
            }
            KeyCode::Backspace => {
                self.input_buf.pop();
            }
            KeyCode::Char(c) => {
                self.input_buf.push(c);
            }
            _ => {}
        }
        Action::None
    }

    fn move_down(&mut self) {
        match self.focus {
            Focus::Remotes => {
                if !self.remotes.is_empty() {
                    self.selected_remote = (self.selected_remote + 1) % self.remotes.len();
                    self.selected_transfer = 0;
                }
            }
            Focus::Transfers => {
                let len = self.current_transfers().len();
                if len > 0 {
                    self.selected_transfer = (self.selected_transfer + 1) % len;
                }
            }
            Focus::Messages => {}
        }
    }

    fn move_up(&mut self) {
        match self.focus {
            Focus::Remotes => {
                if !self.remotes.is_empty() {
                    self.selected_remote = self
                        .selected_remote
                        .checked_sub(1)
                        .unwrap_or(self.remotes.len() - 1);
                    self.selected_transfer = 0;
                }
            }
            Focus::Transfers => {
                let len = self.current_transfers().len();
                if len > 0 {
                    self.selected_transfer =
                        self.selected_transfer.checked_sub(1).unwrap_or(len - 1);
                }
            }
            Focus::Messages => {}
        }
    }
}
