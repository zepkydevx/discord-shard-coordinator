use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use crate::shard::{ShardEvent, ShardId, ShardInfo, ShardStatus};

pub struct Coordinator {
    shards: HashMap<ShardId, ShardInfo>,
    heartbeat_timeout: Duration,
    events: broadcast::Sender<ShardEvent>,
}

impl Coordinator {
    pub fn new(heartbeat_timeout: Duration) -> Self {
        let (events, _) = broadcast::channel(128);
        Self {
            shards: HashMap::new(),
            heartbeat_timeout,
            events,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ShardEvent> {
        self.events.subscribe()
    }

    pub fn register_shard(&mut self, id: ShardId, now: Instant) {
        self.shards.insert(id, ShardInfo::new(id, now));
        let _ = self.events.send(ShardEvent::Registered(id));
    }

    pub fn record_heartbeat(&mut self, id: ShardId, now: Instant) {
        if let Some(shard) = self.shards.get_mut(&id) {
            shard.last_heartbeat = now;
            if shard.status != ShardStatus::Connected {
                shard.status = ShardStatus::Connected;
                let _ = self.events.send(ShardEvent::Reconnected(id));
            }
            let _ = self.events.send(ShardEvent::HeartbeatReceived(id));
        }
    }

    // Marks any shard past heartbeat_timeout as dead. Returns the ids that
    // just changed so the caller can trigger a reconnect.
    pub fn sweep_dead_shards(&mut self, now: Instant) -> Vec<ShardId> {
        let mut newly_dead = Vec::new();
        for shard in self.shards.values_mut() {
            let elapsed = now.duration_since(shard.last_heartbeat);
            if elapsed > self.heartbeat_timeout && shard.status != ShardStatus::Dead {
                shard.status = ShardStatus::Dead;
                newly_dead.push(shard.id);
            }
        }
        for id in &newly_dead {
            let _ = self.events.send(ShardEvent::MarkedDead(*id));
        }
        newly_dead
    }

    pub fn status_of(&self, id: ShardId) -> Option<ShardStatus> {
        self.shards.get(&id).map(|s| s.status)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}
