use std::time::{Duration, Instant};

use discord_shard_coordinator::coordinator::Coordinator;
use discord_shard_coordinator::eventbus::{EventBus, FakeEventBus};
use discord_shard_coordinator::shard::{ShardEvent, ShardStatus};

#[test]
fn a_shard_starts_connected() {
    let mut coordinator = Coordinator::new(Duration::from_millis(100));
    let now = Instant::now();
    coordinator.register_shard(0, now);

    assert_eq!(coordinator.status_of(0), Some(ShardStatus::Connected));
}

#[test]
fn a_silent_shard_is_marked_dead_after_the_timeout() {
    let timeout = Duration::from_millis(100);
    let mut coordinator = Coordinator::new(timeout);
    let start = Instant::now();
    coordinator.register_shard(0, start);

    let still_fine = start + Duration::from_millis(50);
    assert!(coordinator.sweep_dead_shards(still_fine).is_empty());
    assert_eq!(coordinator.status_of(0), Some(ShardStatus::Connected));

    let too_late = start + Duration::from_millis(150);
    assert_eq!(coordinator.sweep_dead_shards(too_late), vec![0]);
    assert_eq!(coordinator.status_of(0), Some(ShardStatus::Dead));
}

#[test]
fn a_heartbeat_resets_the_timeout() {
    let timeout = Duration::from_millis(100);
    let mut coordinator = Coordinator::new(timeout);
    let start = Instant::now();
    coordinator.register_shard(0, start);

    let midpoint = start + Duration::from_millis(80);
    coordinator.record_heartbeat(0, midpoint);

    let after_midpoint = midpoint + Duration::from_millis(80);
    assert!(coordinator.sweep_dead_shards(after_midpoint).is_empty());
    assert_eq!(coordinator.status_of(0), Some(ShardStatus::Connected));
}

#[test]
fn a_reconnected_shard_goes_back_to_connected() {
    let timeout = Duration::from_millis(100);
    let mut coordinator = Coordinator::new(timeout);
    let start = Instant::now();
    coordinator.register_shard(0, start);

    coordinator.sweep_dead_shards(start + Duration::from_millis(150));
    assert_eq!(coordinator.status_of(0), Some(ShardStatus::Dead));

    coordinator.record_heartbeat(0, start + Duration::from_millis(200));
    assert_eq!(coordinator.status_of(0), Some(ShardStatus::Connected));
}

#[test]
fn sweeping_ignores_shards_that_never_registered() {
    let mut coordinator = Coordinator::new(Duration::from_millis(100));
    assert_eq!(coordinator.status_of(99), None);
    assert!(coordinator.sweep_dead_shards(Instant::now()).is_empty());
}

#[tokio::test]
async fn fake_event_bus_records_published_events() {
    let bus = FakeEventBus::default();
    bus.publish(&ShardEvent::MarkedDead(3)).await.unwrap();

    let published = bus.published.lock().unwrap();
    assert_eq!(published.as_slice(), &[ShardEvent::MarkedDead(3)]);
}
