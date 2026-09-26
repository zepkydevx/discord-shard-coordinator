use anyhow::Result;
use async_trait::async_trait;

use crate::shard::ShardEvent;

#[async_trait]
pub trait EventBus: Send + Sync {
    async fn publish(&self, event: &ShardEvent) -> Result<()>;
}

pub struct RedisEventBus {
    client: redis::Client,
    channel: String,
}

impl RedisEventBus {
    pub fn new(redis_url: &str, channel: impl Into<String>) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            client,
            channel: channel.into(),
        })
    }
}

#[async_trait]
impl EventBus for RedisEventBus {
    async fn publish(&self, event: &ShardEvent) -> Result<()> {
        use redis::AsyncCommands;

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let payload = serde_json::to_string(event)?;
        let _: () = conn.publish(&self.channel, payload).await?;
        Ok(())
    }
}

// In-memory stand-in for tests and the demo binary. No Redis required.
#[derive(Default)]
pub struct FakeEventBus {
    pub published: std::sync::Mutex<Vec<ShardEvent>>,
}

#[async_trait]
impl EventBus for FakeEventBus {
    async fn publish(&self, event: &ShardEvent) -> Result<()> {
        self.published.lock().unwrap().push(event.clone());
        Ok(())
    }
}
