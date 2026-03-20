use crate::tui::app::{App, Focus, InputMode};
use bytesize::ByteSize;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};
use warpinator_lib::types::message;
use warpinator_lib::types::remote::{RemoteConnectionError, RemoteState};
use warpinator_lib::types::transfer::TransferState;

pub fn draw(f: &mut Frame, app: &App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),    // top: remotes + transfers/messages
            Constraint::Length(6), // bottom: log
            Constraint::Length(3), // bottom: status/input bar
        ])
        .split(f.area());

    // Split top horizontally: remotes | transfers/messages
    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(32), // Remotes pane fixed width
            Constraint::Min(10),    // Transfers/messages take the rest
        ])
        .split(root[0]);

    // Split right pane horizontally: transfers | messages (messages smaller)
    let right = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(30),    // Transfers take most space
            Constraint::Length(32), // Messages pane fixed width
        ])
        .split(top[1]);

    draw_remotes(f, app, top[0]);
    draw_transfers(f, app, right[0]);
    draw_messages(f, app, right[1]);
    draw_log(f, app, root[1]);
    draw_statusbar(f, app, root[2]);
}

fn remote_state_span(state: &'_ RemoteState) -> Span<'_> {
    match state {
        RemoteState::Disconnected => {
            Span::styled("[disconnected]", Style::default().fg(Color::DarkGray))
        }
        RemoteState::Connecting => Span::styled("[connecting]", Style::default().fg(Color::Yellow)),
        RemoteState::AwaitingDuplex => {
            Span::styled("[awaiting]", Style::default().fg(Color::Yellow))
        }
        RemoteState::Connected => Span::styled("[connected]", Style::default().fg(Color::Green)),
        RemoteState::Error(err) => {
            let msg = match err {
                RemoteConnectionError::SslError => "[ssl error]",
                RemoteConnectionError::GroupCodeMismatch => "[group mismatch]",
                RemoteConnectionError::NoCertificate => "[no cert]",
                RemoteConnectionError::DuplexError => "[duplex err]",
            };
            Span::styled(msg, Style::default().fg(Color::Red))
        }
    }
}

fn transfer_state_span(state: &'_ TransferState) -> Span<'_> {
    match state {
        TransferState::Initializing => Span::styled("[init]", Style::default().fg(Color::DarkGray)),
        TransferState::WaitingPermission => {
            Span::styled("[waiting]", Style::default().fg(Color::Yellow))
        }
        TransferState::InProgress => {
            Span::styled("[in progress]", Style::default().fg(Color::Green))
        }
        TransferState::Paused => Span::styled("[paused]", Style::default().fg(Color::Yellow)),
        TransferState::Completed => Span::styled("[done]", Style::default().fg(Color::Green)),
        TransferState::Canceled => Span::styled("[canceled]", Style::default().fg(Color::DarkGray)),
        TransferState::Denied => Span::styled("[denied]", Style::default().fg(Color::Red)),
        TransferState::Failed(_) => Span::styled("[failed]", Style::default().fg(Color::Red)),
    }
}

fn draw_remotes(f: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Remotes;
    let block = Block::default()
        .title(" Remotes ")
        .borders(Borders::ALL)
        .border_style(focus_style(focused));

    let items: Vec<ListItem> = app
        .remotes
        .iter()
        .map(|remote| {
            let state_span = remote_state_span(&remote.state);
            ListItem::new(Line::from(vec![
                state_span,
                Span::raw(" "),
                Span::styled(remote.display_name.as_str(), Style::default()),
            ]))
        })
        .collect();

    let mut state = ListState::default();
    if !app.remotes.is_empty() {
        state.select(Some(app.selected_remote));
    }

    let list = List::new(items)
        .block(block)
        .highlight_style(highlight_style())
        .highlight_symbol("> ");

    f.render_stateful_widget(list, area, &mut state);
}

fn draw_transfers(f: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Transfers;
    let title = match app.current_remote() {
        Some(remote) => format!(" Transfers — {} ", remote.display_name),
        None => " Transfers ".to_string(),
    };

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(focus_style(focused));

    let transfers = app.current_transfers();
    let items: Vec<ListItem> = transfers
        .iter()
        .map(|transfer| {
            let desc = if let Some(name) = &transfer.single_name {
                // Single file: show name (size)
                format!("{} ({})", name, ByteSize(transfer.total_bytes).to_string())
            } else {
                // Directory or multiple files: show up to 3 names, then count
                let mut names = transfer
                    .entry_names
                    .iter()
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>();
                let cutoff = transfer.entry_names.len() > 3;
                if cutoff {
                    names.push("...".to_string());
                }
                let joined = names.join(", ");
                format!(
                    "{} {} files ({})",
                    joined,
                    transfer.file_count,
                    ByteSize(transfer.total_bytes).to_string()
                )
            };

            let state_span = transfer_state_span(&transfer.state);

            ListItem::new(Line::from(vec![
                Span::raw(" "),
                state_span,
                Span::raw(" "),
                Span::styled(desc, Style::default()),
            ]))
        })
        .collect();

    let mut state = ListState::default();
    if !transfers.is_empty() {
        state.select(Some(app.selected_transfer));
    }

    let list = List::new(items)
        .block(block)
        .highlight_style(highlight_style())
        .highlight_symbol("> ");

    f.render_stateful_widget(list, area, &mut state);
}

fn draw_messages(f: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Messages;
    let title = match app.current_remote() {
        Some(remote) => format!(" Messages — {} ", remote.display_name),
        None => " Messages ".to_string(),
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(focus_style(focused));

    let messages = app.current_messages();
    let items: Vec<ListItem> = messages
        .iter()
        .map(|msg| {
            let dir = match msg.direction {
                message::Direction::Sent => "→",
                message::Direction::Received => "←",
            };
            let line = Line::from(vec![
                Span::styled(dir, Style::default().fg(Color::Yellow)),
                Span::raw(" "),
                Span::raw(&msg.content),
            ]);
            ListItem::new(line)
        })
        .collect();

    // Always show at least one empty item if no messages
    let items = if items.is_empty() {
        vec![ListItem::new("")]
    } else {
        items
    };

    let list = List::new(items).block(block);
    f.render_widget(list, area);
}

fn draw_log(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default().title(" Log ").borders(Borders::ALL);
    let inner_height = area.height.saturating_sub(2) as usize;

    let lines: Vec<Line> = app
        .log
        .iter()
        .rev()
        .take(inner_height)
        .rev()
        .map(|s| Line::from(s.as_str()))
        .collect();

    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

fn draw_statusbar(f: &mut Frame, app: &App, area: Rect) {
    let content = match &app.input_mode {
        Some(InputMode::FilePath) => {
            format!(" Send file: {}_", app.input_buf)
        }
        Some(InputMode::Message) => {
            format!(" Message: {}_", app.input_buf)
        }
        None => " j/k: nav  Tab: pane  s: send file  m: message  a: accept  r: reject  p: pause  x: stop  q: quit".to_string(),
    };

    let style = match app.input_mode {
        Some(_) => Style::default().fg(Color::Yellow),
        None => Style::default().fg(Color::DarkGray),
    };

    let bar = Paragraph::new(content)
        .style(style)
        .block(Block::default().borders(Borders::ALL));

    f.render_widget(bar, area);
}

fn focus_style(focused: bool) -> Style {
    if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    }
}

fn highlight_style() -> Style {
    Style::default()
        .fg(Color::Black)
        .bg(Color::Cyan)
        .add_modifier(Modifier::BOLD)
}
