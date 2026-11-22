use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use tracing::{error, trace, warn};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct FakeIpMappings {
    #[serde(default)]
    ip_to_host: HashMap<String, String>,
    #[serde(default)]
    host_to_ip: HashMap<String, String>,
}

impl FakeIpMappings {
    fn insert(&mut self, ip: &str, host: &str) {
        if let Some(old_host) = self.ip_to_host.insert(ip.into(), host.into())
            && old_host != host
            && self.host_to_ip.get(&old_host).is_some_and(|old| old == ip)
        {
            self.host_to_ip.remove(&old_host);
        }
        if let Some(old_ip) = self.host_to_ip.insert(host.into(), ip.into())
            && old_ip != ip
            && self.ip_to_host.get(&old_ip).is_some_and(|old| old == host)
        {
            self.ip_to_host.remove(&old_ip);
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct Db {
    #[serde(default)]
    selected: HashMap<String, String>,
    // Keep the existing top-level IPv4 maps readable across upgrades.
    #[serde(flatten)]
    fake_ipv4: FakeIpMappings,
    #[serde(default)]
    fake_ipv6: FakeIpMappings,
    #[serde(default)]
    smart_stats: HashMap<String, crate::proxy::group::smart::state::SmartStateData>,
    #[serde(default)]
    smart_policy_priority: HashMap<String, String>,
}

impl Db {
    fn fake_ips(&self, ipv6: bool) -> &FakeIpMappings {
        if ipv6 {
            &self.fake_ipv6
        } else {
            &self.fake_ipv4
        }
    }

    fn fake_ips_mut(&mut self, ipv6: bool) -> &mut FakeIpMappings {
        if ipv6 {
            &mut self.fake_ipv6
        } else {
            &mut self.fake_ipv4
        }
    }
}

#[derive(Clone)]
pub struct ThreadSafeCacheFile(Arc<tokio::sync::RwLock<CacheFile>>);

impl ThreadSafeCacheFile {
    pub fn new(path: &str, store_selected: bool) -> Self {
        Self::with_persistence(path, store_selected, false)
    }

    pub(crate) fn with_persistence(
        path: &str,
        store_selected: bool,
        store_fake_ip: bool,
    ) -> Self {
        let store = Arc::new(tokio::sync::RwLock::new(CacheFile::new(
            path,
            store_selected,
        )));

        if store_selected || store_fake_ip {
            let path = path.to_string();
            let weak_store = Arc::downgrade(&store);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                    let Some(store) = weak_store.upgrade() else {
                        break;
                    };
                    let r = store.read().await;
                    let db = r.db.clone();
                    drop(r);

                    let s = match serde_yaml::to_string(&db) {
                        Ok(s) => s,
                        Err(e) => {
                            error!("failed to serialize cache file: {}", e);
                            continue;
                        }
                    };

                    match tokio::fs::write(&path, s).await {
                        Err(e) => {
                            error!("failed to write cache file: {}", e);
                        }
                        _ => {
                            trace!("cache file flushed to {}", path);
                        }
                    }
                }
            });
        }

        Self(store)
    }

    pub async fn set_selected(&self, group: &str, server: &str) {
        let mut g = self.0.write().await;
        if g.store_selected() {
            g.set_selected(group, server);
        }
    }

    pub async fn get_selected(&self, group: &str) -> Option<String> {
        let g = self.0.read().await;
        if g.store_selected() {
            g.db.selected.get(group).cloned()
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub async fn get_selected_map(&self) -> HashMap<String, String> {
        let g = self.0.read().await;
        if g.store_selected() {
            g.get_selected_map()
        } else {
            HashMap::new()
        }
    }

    pub async fn set_ip_to_host(&self, ip: &str, host: &str) {
        let mut cache = self.0.write().await;
        cache
            .db
            .fake_ips_mut(ip.parse::<std::net::Ipv6Addr>().is_ok())
            .ip_to_host
            .insert(ip.into(), host.into());
    }

    pub async fn set_host_to_ip(&self, host: &str, ip: &str) {
        let mut cache = self.0.write().await;
        cache
            .db
            .fake_ips_mut(ip.parse::<std::net::Ipv6Addr>().is_ok())
            .host_to_ip
            .insert(host.into(), ip.into());
    }

    pub async fn get_fake_ip(&self, ip_or_host: &str) -> Option<String> {
        let cache = self.0.read().await;
        for maps in [&cache.db.fake_ipv4, &cache.db.fake_ipv6] {
            if let Some(result) = maps
                .ip_to_host
                .get(ip_or_host)
                .or_else(|| maps.host_to_ip.get(ip_or_host))
            {
                return Some(result.clone());
            }
        }
        None
    }

    pub async fn delete_fake_ip_pair(&self, ip: &str, host: &str) {
        let mut cache = self.0.write().await;
        let maps = cache
            .db
            .fake_ips_mut(ip.parse::<std::net::Ipv6Addr>().is_ok());
        maps.ip_to_host.remove(ip);
        maps.host_to_ip.remove(host);
    }

    pub async fn flush_fake_ip(&self) {
        let mut cache = self.0.write().await;
        cache.db.fake_ipv4 = FakeIpMappings::default();
        cache.db.fake_ipv6 = FakeIpMappings::default();
    }

    pub(crate) async fn set_fake_ip(&self, ip: std::net::IpAddr, host: &str) {
        self.0
            .write()
            .await
            .db
            .fake_ips_mut(ip.is_ipv6())
            .insert(&ip.to_string(), host);
    }

    pub(crate) async fn fake_ip_for(
        &self,
        host: &str,
        ipv6: bool,
    ) -> Option<std::net::IpAddr> {
        let db = self.0.read().await;
        let mappings = db.db.fake_ips(ipv6);
        let ip = mappings.host_to_ip.get(host)?;
        if mappings.ip_to_host.get(ip)? != host {
            return None;
        }
        ip.parse().ok()
    }

    pub(crate) async fn fake_host_for(
        &self,
        ip: std::net::IpAddr,
    ) -> Option<String> {
        let db = self.0.read().await;
        let mappings = db.db.fake_ips(ip.is_ipv6());
        let ip = ip.to_string();
        let host = mappings.ip_to_host.get(&ip)?;
        (mappings.host_to_ip.get(host) == Some(&ip)).then(|| host.clone())
    }

    pub(crate) async fn flush_fake_ip_family(&self, ipv6: bool) {
        *self.0.write().await.db.fake_ips_mut(ipv6) = FakeIpMappings::default();
    }

    /// Store smart proxy group statistics
    pub async fn set_smart_stats(
        &self,
        group_name: &str,
        stats: crate::proxy::group::smart::state::SmartStateData,
    ) {
        let mut g = self.0.write().await;
        g.set_smart_stats(group_name, stats);
    }

    /// Get smart proxy group statistics
    pub async fn get_smart_stats(
        &self,
        group_name: &str,
    ) -> Option<crate::proxy::group::smart::state::SmartStateData> {
        let g = self.0.read().await;
        g.get_smart_stats(group_name)
    }
}

struct CacheFile {
    db: Db,

    store_selected: bool,
}

impl CacheFile {
    pub fn new(path: &str, store_selected: bool) -> Self {
        let db = match std::fs::read_to_string(path) {
            Ok(s) => match serde_yaml::from_str(&s) {
                Ok(db) => db,
                Err(e) => {
                    error!(
                        "failed to parse cache file: {}, initializing a new one",
                        e
                    );
                    Db::default()
                }
            },
            Err(e) => {
                warn!("failed to read cache file: {}, initializing a new one", e);
                Db::default()
            }
        };

        Self { db, store_selected }
    }

    pub fn store_selected(&self) -> bool {
        self.store_selected
    }

    pub fn set_selected(&mut self, group: &str, server: &str) {
        self.db
            .selected
            .insert(group.to_string(), server.to_string());
    }

    pub fn get_selected_map(&self) -> HashMap<String, String> {
        self.db.selected.clone()
    }

    pub fn set_smart_stats(
        &mut self,
        group_name: &str,
        stats: crate::proxy::group::smart::state::SmartStateData,
    ) {
        self.db.smart_stats.insert(group_name.to_string(), stats);
    }

    pub fn get_smart_stats(
        &self,
        group_name: &str,
    ) -> Option<crate::proxy::group::smart::state::SmartStateData> {
        self.db.smart_stats.get(group_name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn fake_ip_cache_reopens_legacy_and_dual_stack_leases() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.yaml");
        let host = "cdn.example";
        let v4 = "198.18.0.5".parse().unwrap();
        let v6 = "fdfe:dcba:9876::4".parse().unwrap();
        std::fs::write(
            &path,
            "ip_to_host: {198.18.0.5: cdn.example}\nhost_to_ip: {cdn.example: \
             198.18.0.5}",
        )
        .unwrap();
        let open = || ThreadSafeCacheFile::new(path.to_str().unwrap(), false);
        let cache = ThreadSafeCacheFile::with_persistence(
            path.to_str().unwrap(),
            false,
            true,
        );
        assert_eq!(cache.fake_ip_for(host, false).await, Some(v4));
        cache.set_fake_ip(v6, host).await;
        // Wait for the production cache writer with store-selected disabled.
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        tokio::time::resume();
        let reopened =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let reopened = open();
                    if reopened.fake_ip_for(host, true).await == Some(v6) {
                        break reopened;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        drop(cache);
        for ip in [v4, v6] {
            assert_eq!(reopened.fake_ip_for(host, ip.is_ipv6()).await, Some(ip));
            assert_eq!(reopened.fake_host_for(ip).await.as_deref(), Some(host));
        }
        reopened.set_fake_ip(v6, "other.example").await;
        assert_eq!(reopened.fake_ip_for(host, true).await, None);
        reopened.flush_fake_ip_family(true).await;
        assert_eq!(reopened.fake_host_for(v6).await, None);
        assert_eq!(reopened.fake_host_for(v4).await.as_deref(), Some(host));
    }
}
