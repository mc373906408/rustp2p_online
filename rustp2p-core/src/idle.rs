use std::hash::Hash;
use std::time::{Duration, Instant};

use crate::route_table::RouteKey;
use crate::route_table::{Route, RouteTable};

pub struct IdleRouteManager<PeerID> {
    read_idle: Duration,
    route_table: RouteTable<PeerID>,
}

impl<PeerID: Hash + Eq + Clone> IdleRouteManager<PeerID> {
    pub fn new(read_idle: Duration, route_table: RouteTable<PeerID>) -> IdleRouteManager<PeerID> {
        Self {
            read_idle,
            route_table,
        }
    }
    /// Take the timeout routes from the managed route_table
    pub async fn next_idle(&self) -> (PeerID, Route, Instant) {
        loop {
            let sleep_for = if let Some((peer_id, route, instant)) = self.route_table.oldest_route()
            {
                let elapsed = instant.elapsed();
                if elapsed >= self.read_idle {
                    return (peer_id, route, instant);
                }
                // Sleep until the oldest route expires, not the time it has
                // already been alive.
                self.read_idle - elapsed
            } else {
                self.read_idle / 3
            };
            tokio::time::sleep(sleep_for.max(Duration::from_millis(1))).await;
        }
    }
    pub fn delay(&self, peer_id: &PeerID, route_key: &RouteKey) -> bool {
        self.route_table.update_read_time(peer_id, route_key)
    }
    pub fn remove_route(&self, peer_id: &PeerID, route_key: &RouteKey) {
        self.route_table.remove_route(peer_id, route_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::route_table::{Protocol, RouteKey};

    fn key(port: u16) -> RouteKey {
        RouteKey::new(
            Protocol::UDP,
            "127.0.0.1:1000".parse().unwrap(),
            format!("127.0.0.1:{port}").parse().unwrap(),
        )
    }

    #[tokio::test]
    async fn expired_route_is_returned() {
        let table = RouteTable::<u32>::default();
        table.add_route(1, Route::from_default_rt(key(2000), 0));
        let manager = IdleRouteManager::new(Duration::ZERO, table);
        let (peer_id, route, _) = manager.next_idle().await;
        assert_eq!(peer_id, 1);
        assert_eq!(route.route_key(), key(2000));
    }

    #[tokio::test]
    async fn fresh_route_is_not_returned_early() {
        let table = RouteTable::<u32>::default();
        table.add_route(1, Route::from_default_rt(key(2000), 0));
        let manager = IdleRouteManager::new(Duration::from_secs(3600), table);
        let result = tokio::time::timeout(Duration::from_millis(100), manager.next_idle()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn removed_route_is_not_returned() {
        let table = RouteTable::<u32>::default();
        table.add_route(1, Route::from_default_rt(key(2000), 0));
        let manager = IdleRouteManager::new(Duration::ZERO, table.clone());
        manager.remove_route(&1, &key(2000));
        let result = tokio::time::timeout(Duration::from_millis(50), manager.next_idle()).await;
        assert!(result.is_err());
    }
}
