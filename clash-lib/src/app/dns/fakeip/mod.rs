use std::net;

use crate::common::trie;

use async_trait::async_trait;
use tokio::sync::RwLock;

mod file_store;
mod mem_store;

pub use file_store::FileStore;
pub use mem_store::InMemStore;

#[async_trait]
pub trait Store: Sync + Send {
    async fn get_by_host(&mut self, host: &str) -> Option<net::IpAddr>;
    async fn get_by_ip(&mut self, ip: net::IpAddr) -> Option<String>;
    async fn put_by_ip(&mut self, ip: net::IpAddr, host: &str);
    async fn exist(&mut self, ip: net::IpAddr) -> bool;
    #[allow(dead_code)]
    async fn copy_to(&self, store: &mut Box<dyn Store>);
    async fn flush(&mut self);
}

pub struct FakeDns {
    pub ipv4: Option<RwLock<Pool>>,
    pub ipv6: Option<RwLock<Pool>>,
    pub skipped_hostnames: trie::StringTrie<bool>,
}

impl FakeDns {
    pub fn should_skip(&self, domain: &str) -> bool {
        self.skipped_hostnames.search(domain).is_some()
    }

    pub async fn lookup_v4(&self, host: &str) -> Option<net::Ipv4Addr> {
        match self.ipv4.as_ref()?.write().await.lookup(host).await {
            net::IpAddr::V4(ip) => Some(ip),
            _ => unreachable!("IPv4 pool"),
        }
    }

    pub async fn lookup_v6(&self, host: &str) -> Option<net::Ipv6Addr> {
        match self.ipv6.as_ref()?.write().await.lookup(host).await {
            net::IpAddr::V6(ip) => Some(ip),
            _ => unreachable!("IPv6 pool"),
        }
    }

    pub fn pool(&self, ip: net::IpAddr) -> Option<&RwLock<Pool>> {
        if ip.is_ipv4() {
            self.ipv4.as_ref()
        } else {
            self.ipv6.as_ref()
        }
    }
}

pub struct Pool {
    ipnet: ipnet::IpNet,
    first: u128,
    last: u128,
    next: u128,
    store: Box<dyn Store>,
}

impl Pool {
    /// Prefix sizes are validated by FakeIpRange before pool construction.
    pub fn new(ipnet: ipnet::IpNet, store: Box<dyn Store>) -> Self {
        let (first, last) = match ipnet {
            ipnet::IpNet::V4(net) => (
                u128::from(net.network().to_bits()) + 2,
                u128::from(net.broadcast().to_bits()),
            ),
            ipnet::IpNet::V6(net) => {
                (net.network().to_bits() + 4, net.broadcast().to_bits() - 1)
            }
        };
        Self {
            ipnet,
            first,
            last,
            next: first,
            store,
        }
    }

    fn take_next(&mut self) -> net::IpAddr {
        let addr = self.next;
        self.next = if addr == self.last {
            self.first
        } else {
            addr + 1
        };
        match self.ipnet {
            ipnet::IpNet::V4(_) => net::Ipv4Addr::from_bits(addr as u32).into(),
            ipnet::IpNet::V6(_) => net::Ipv6Addr::from_bits(addr).into(),
        }
    }

    fn allocatable(&self, ip: net::IpAddr) -> bool {
        let addr = match (self.ipnet, ip) {
            (ipnet::IpNet::V4(_), net::IpAddr::V4(ip)) => u128::from(ip.to_bits()),
            (ipnet::IpNet::V6(_), net::IpAddr::V6(ip)) => ip.to_bits(),
            _ => return false,
        };
        (self.first..=self.last).contains(&addr)
    }

    pub async fn lookup(&mut self, host: &str) -> net::IpAddr {
        if let Some(ip) = self.store.get_by_host(host).await
            && self.allocatable(ip)
        {
            return ip;
        }

        let start = self.next;
        loop {
            let ip = self.take_next();
            if !self.store.exist(ip).await {
                self.store.put_by_ip(ip, host).await;
                return ip;
            }
            if self.next == start {
                // The pool is full. Reuse the oldest address in allocation
                // order.
                let ip = self.take_next();
                self.store.put_by_ip(ip, host).await;
                return ip;
            }
        }
    }

    pub async fn reverse_lookup(&mut self, ip: net::IpAddr) -> Option<String> {
        if self.allocatable(ip) {
            self.store.get_by_ip(ip).await
        } else {
            None
        }
    }

    pub async fn flush(&mut self) {
        self.next = self.first;
        self.store.flush().await;
    }

    pub async fn is_fake_ip(&mut self, ip: net::IpAddr) -> bool {
        if ip.is_multicast()
            || matches!(ip, net::IpAddr::V4(ip) if ip.is_broadcast())
        {
            return false;
        }
        self.allocatable(ip) && self.store.exist(ip).await
    }
}

#[cfg(test)]
mod tests {
    use std::{net, sync::Arc};

    use crate::{app::dns::fakeip::mem_store::InMemStore, common::trie};

    use super::{FakeDns, Pool};

    #[tokio::test]
    async fn test_ipv6_pool_recycles_at_prefix_boundary() {
        let net = "fdff:ffff:ffff:ffff:ffff:ffff:ffff:fff9/125"
            .parse()
            .unwrap();
        let mut pool = Pool::new(net, Box::new(InMemStore::new(8)));
        let first = pool.lookup("first.test").await;
        assert_eq!(
            first,
            "fdff:ffff:ffff:ffff:ffff:ffff:ffff:fffc"
                .parse::<net::IpAddr>()
                .unwrap()
        );
        pool.lookup("second.test").await;
        let last = pool.lookup("third.test").await;
        assert_eq!(
            last,
            "fdff:ffff:ffff:ffff:ffff:ffff:ffff:fffe"
                .parse::<net::IpAddr>()
                .unwrap()
        );
        assert_eq!(pool.lookup("fourth.test").await, first);
        assert_eq!(
            pool.reverse_lookup(first).await.as_deref(),
            Some("fourth.test")
        );
        assert_ne!(pool.lookup("first.test").await, first);
    }

    #[tokio::test]
    async fn test_inmem_basic() {
        let ipnet = "192.168.0.0/29".parse::<ipnet::IpNet>().unwrap();
        let store = Box::new(InMemStore::new(10));
        let mut pool = Pool::new(ipnet, store);

        let first = pool.lookup("foo.com").await;
        let last = pool.lookup("bar.com").await;

        let bar = pool.reverse_lookup(last).await;

        assert_eq!(first, net::IpAddr::from([192, 168, 0, 2]));
        assert_eq!(
            pool.lookup("foo.com").await,
            net::IpAddr::from([192, 168, 0, 2])
        );
        assert_eq!(last, net::IpAddr::from([192, 168, 0, 3]));
        assert!(bar.is_some());
        assert_eq!(bar, Some("bar.com".into()));
        assert_eq!(pool.ipnet.to_string(), ipnet.to_string());
        assert!(pool.is_fake_ip(net::IpAddr::from([192, 168, 0, 3])).await);
        assert!(!pool.is_fake_ip(net::IpAddr::from([192, 168, 0, 4])).await);
        assert!(!pool.is_fake_ip("::1".parse().unwrap()).await);
    }

    #[tokio::test]
    async fn test_inmem_cycle_used() {
        let store = Box::new(InMemStore::new(10));

        let ipnet = "192.168.0.0/29".parse::<ipnet::IpNet>().unwrap();
        let mut pool = Pool::new(ipnet, store);

        let foo = pool.lookup("foo.com").await;
        let bar = pool.lookup("bar.com").await;

        // Fill all 6 IPs in the /29 pool (min=192.168.0.2, max=192.168.0.7).
        // foo and bar already use 2, so 4 more are needed to fill the pool.
        for i in 0..4 {
            pool.lookup(&format!("{}.com", i)).await;
        }

        // Pool is now full — baz must recycle foo's IP (oldest entry).
        let baz = pool.lookup("baz.com").await;
        // foo.com's forward mapping was cleaned up by put_by_ip, so the next
        // lookup allocates a fresh IP (bar's recycled IP).
        let next = pool.lookup("foo.com").await;
        assert_eq!(foo, baz);
        assert_eq!(next, bar);
    }

    #[tokio::test]
    async fn test_pool_skip() {
        let mut tree = trie::StringTrie::new();
        tree.insert("example.com", Arc::new(false));

        let pool = FakeDns {
            ipv4: None,
            ipv6: None,
            skipped_hostnames: tree,
        };

        assert!(pool.should_skip("example.com"));
        assert!(!pool.should_skip("foo.com"));
    }

    #[tokio::test]
    async fn test_pool_max_cache_size() {
        let store = Box::new(InMemStore::new(2));

        let ipnet = "192.168.0.0/24".parse::<ipnet::IpNet>().unwrap();
        let mut pool = Pool::new(ipnet, store);

        let first = pool.lookup("foo.com").await;

        pool.lookup("bar.com").await;
        pool.lookup("baz.com").await;
        let next = pool.lookup("foo.com").await;

        assert_ne!(first, next);
    }

    #[tokio::test]
    #[ignore = "copy not implemented"]
    async fn test_pool_clone() {
        let store = Box::new(InMemStore::new(2));

        let ipnet = "192.168.0.0/24".parse::<ipnet::IpNet>().unwrap();
        let mut pool = Pool::new(ipnet, store);

        let first = pool.lookup("foo.com").await;
        let last = pool.lookup("bar.com").await;
        assert_eq!(first, net::IpAddr::from([192, 168, 0, 2]));
        assert_eq!(last, net::IpAddr::from([192, 168, 0, 3]));

        let store = Box::new(InMemStore::new(2));

        let mut new_pool = Pool::new(ipnet, store);

        pool.store.copy_to(&mut new_pool.store).await;

        assert!(new_pool.reverse_lookup(first).await.is_some());
        assert!(new_pool.reverse_lookup(last).await.is_some());
    }

    #[tokio::test]
    async fn test_is_fake_ip_excludes_broadcast_and_unallocated() {
        let store = Box::new(InMemStore::new(10));

        // Use 198.18.0.0/16 (the default fake-ip-range) to mirror the real
        // production setup described in the bug report, where the TUN gateway
        // is 198.18.0.1/24 and its subnet broadcast 198.18.0.255 fell inside
        // the wider /16 fake-ip range.
        let ipnet = "198.18.0.0/16".parse::<ipnet::IpNet>().unwrap();
        let mut pool = Pool::new(ipnet, store);

        // Allocate one real fake IP.
        let allocated = pool.lookup("foo.com").await;
        assert!(
            pool.is_fake_ip(allocated).await,
            "allocated IP must be fake"
        );

        // Directed broadcast for the /24 TUN subnet (198.18.0.0/24) – never
        // allocated, yet it falls inside the /16 range.
        let directed_broadcast: net::IpAddr = "198.18.0.255".parse().unwrap();
        assert!(
            !pool.is_fake_ip(directed_broadcast).await,
            "directed broadcast must not be treated as a fake IP"
        );

        // Global broadcast must never be a fake IP.
        let global_broadcast: net::IpAddr = "255.255.255.255".parse().unwrap();
        assert!(
            !pool.is_fake_ip(global_broadcast).await,
            "255.255.255.255 must not be a fake IP"
        );

        // A multicast address must never be a fake IP.
        let multicast: net::IpAddr = "224.0.0.1".parse().unwrap();
        assert!(
            !pool.is_fake_ip(multicast).await,
            "multicast must not be a fake IP"
        );

        // An IP in the range that was never allocated must not be fake.
        let unallocated: net::IpAddr = "198.18.1.1".parse().unwrap();
        assert!(
            !pool.is_fake_ip(unallocated).await,
            "unallocated in-range IP must not be treated as a fake IP"
        );
    }

    #[tokio::test]
    async fn test_fake_ip_flush() {
        let store = Box::new(InMemStore::new(10));
        let ipnet = "198.18.0.0/16".parse::<ipnet::IpNet>().unwrap();
        let mut pool = Pool::new(ipnet, store);

        let ip1 = pool.lookup("foo.com").await;
        assert!(pool.is_fake_ip(ip1).await);
        assert_eq!(pool.reverse_lookup(ip1).await, Some("foo.com".to_string()));

        pool.flush().await;

        assert!(!pool.is_fake_ip(ip1).await);
        assert_eq!(pool.reverse_lookup(ip1).await, None);

        // After flush, offset resets so lookup allocates from beginning
        let ip2 = pool.lookup("bar.com").await;
        assert_eq!(ip1, ip2);
        assert_eq!(pool.reverse_lookup(ip2).await, Some("bar.com".to_string()));
    }
}
