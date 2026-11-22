use std::net::IpAddr;

use async_trait::async_trait;

use crate::app::profile::ThreadSafeCacheFile;

use super::Store;

pub struct FileStore {
    cache: ThreadSafeCacheFile,
    ipv6: bool,
}

impl FileStore {
    pub fn new(cache: ThreadSafeCacheFile, ipv6: bool) -> Self {
        Self { cache, ipv6 }
    }
}

#[async_trait]
impl Store for FileStore {
    async fn get_by_host(&mut self, host: &str) -> Option<IpAddr> {
        self.cache.fake_ip_for(host, self.ipv6).await
    }

    async fn get_by_ip(&mut self, ip: IpAddr) -> Option<String> {
        self.cache.fake_host_for(ip).await
    }

    async fn put_by_ip(&mut self, ip: IpAddr, host: &str) {
        self.cache.set_fake_ip(ip, host).await;
    }

    async fn exist(&mut self, ip: IpAddr) -> bool {
        self.cache.fake_host_for(ip).await.is_some()
    }

    async fn copy_to(&self, #[allow(unused)] store: &mut Box<dyn Store>) {
        // NO-OP
    }

    async fn flush(&mut self) {
        self.cache.flush_fake_ip_family(self.ipv6).await;
    }
}
