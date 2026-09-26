use std::time::Instant;

// Discord shard indices are small sequential integers.
pub type ShardId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardStatus {
    Connected,
    Reconnecting,
    Dead,
}

#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub id: ShardId,
    pub status: ShardStatus,
    pub last_heartbeat: Instant,
}

impl ShardInfo {
    pub fn new(id: ShardId, now: Instant) -> Self {
        Self {
            id,
            status: ShardStatus::Connected,
            last_heartbeat: now,
        }
    }
}

// Emitted on every shard state change.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShardEvent {
    Registered(ShardId),
    HeartbeatReceived(ShardId),
    MarkedDead(ShardId),
    Reconnected(ShardId),
}
