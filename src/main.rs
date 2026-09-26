use std::sync::Arc;
use std::time::{Duration, Instant};

use discord_shard_coordinator::coordinator::Coordinator;
use discord_shard_coordinator::eventbus::{EventBus, FakeEventBus};
use discord_shard_coordinator::shard::ShardEvent;

const TOTAL_SHARDS: u32 = 4;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(500);
const SHARD_TO_KILL: u32 = 2;

#[tokio::main]
async fn main() {
    let mut coordinator = Coordinator::new(HEARTBEAT_TIMEOUT);
    let bus: Arc<dyn EventBus> = Arc::new(FakeEventBus::default());

    let now = Instant::now();
    for id in 0..TOTAL_SHARDS {
        coordinator.register_shard(id, now);
    }

    let mut events = coordinator.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            println!("[event] {event:?}");
        }
    });

    println!("Simulating {TOTAL_SHARDS} shards, shard {SHARD_TO_KILL} will go silent...\n");

    for tick in 0..10 {
        tokio::time::sleep(HEARTBEAT_INTERVAL).await;
        let now = Instant::now();

        for id in 0..TOTAL_SHARDS {
            if id == SHARD_TO_KILL && tick >= 2 {
                continue;
            }
            coordinator.record_heartbeat(id, now);
        }

        let dead = coordinator.sweep_dead_shards(now);
        for id in &dead {
            bus.publish(&ShardEvent::MarkedDead(*id)).await.ok();
            println!("shard {id} marked dead, needs reconnect");
        }
    }

    println!("\nFinal state:");
    for id in 0..TOTAL_SHARDS {
        println!("  shard {id}: {:?}", coordinator.status_of(id));
    }
}
