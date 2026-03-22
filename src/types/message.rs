use uuid::Uuid;

use crate::proto::TextMessage;

#[derive(Clone, Debug)]
pub enum Direction {
    Sent,
    Received,
}

#[derive(Clone, Debug)]
pub struct Message {
    pub uuid: String,
    pub remote_uuid: String,
    pub direction: Direction,
    pub timestamp: u64,
    pub content: String,
}

impl Message {
    pub fn new(remote_uuid: String, direction: Direction, content: String) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
            remote_uuid,
            direction,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            content,
        }
    }

    pub fn as_proto(&self, service_id: &str) -> TextMessage {
        TextMessage {
            ident: service_id.to_string(),
            timestamp: self.timestamp,
            message: self.content.clone(),
        }
    }
}

impl From<&TextMessage> for Message {
    fn from(value: &TextMessage) -> Self {
        Message::new(value.ident.clone(), Direction::Received, value.message.clone())
    }
}
