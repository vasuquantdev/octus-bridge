use super::FxDashMap;
use core::hash::Hash;
use nekoton_utils::{Clock, SimpleClock};
use std::fmt::Debug;
use std::time::Duration;

#[macro_export]
macro_rules! from_cache_or_recache {
    ($cache:expr, $cache_key:expr, $fallback:expr, $policy:expr) => {{
        let cache = $cache;
        let cache_key = $cache_key;
        if let Some(cached_value) = cache.get(&cache_key) {
            Ok(cached_value)
        } else {
            let result = $fallback;
            cache.add(cache_key, result.clone(), $policy);
            Ok(result)
        }
    }};
}

#[derive(Clone, Debug)]
pub enum CacheItemPolicy {
    AbsoluteExpiration(Duration),
    NoExpiration,
}

#[derive(Clone)]
pub struct CacheItem<V: Clone + Debug> {
    pub value: V,
    pub policy: CacheItemPolicy,
    pub created: u64,
}

impl<V: Clone + Debug> CacheItem<V> {
    fn new(value: V, policy: CacheItemPolicy, created: u64) -> Self {
        Self {
            value,
            policy,
            created,
        }
    }
}

pub type MemoryCache<K, V> = MemoryCacheBase<K, V, SimpleClock>;

impl<K: Eq + Hash + Clone + Debug, V: Clone + Debug> MemoryCache<K, V> {
    pub fn new(name: &'static str) -> Self {
        Self::with_clock(name, SimpleClock)
    }

    pub fn from_iter<I: IntoIterator<Item = (K, V, CacheItemPolicy)>>(
        name: &'static str,
        iter: I,
    ) -> Self {
        Self::from_iter_with_clock(name, iter, SimpleClock)
    }
}

#[derive(Clone)]
pub struct MemoryCacheBase<K: Eq + Hash + Clone + Debug, V: Clone + Debug, T: Clock> {
    name: &'static str,
    clock: T,
    cache: FxDashMap<K, CacheItem<V>>,
}

impl<K: Eq + Hash + Clone + Debug, V: Clone + Debug, T: Clock> MemoryCacheBase<K, V, T> {
    pub fn with_clock(name: &'static str, clock: T) -> Self {
        Self {
            name,
            clock,
            cache: FxDashMap::default(),
        }
    }

    pub fn from_iter_with_clock<I: IntoIterator<Item = (K, V, CacheItemPolicy)>>(
        name: &'static str,
        iter: I,
        clock: T,
    ) -> Self {
        Self {
            name,
            cache: FxDashMap::from_iter::<Vec<_>>(
                iter.into_iter()
                    .map(|i| (i.0, CacheItem::new(i.1, i.2, clock.now_sec_u64())))
                    .collect(),
            ),
            clock,
        }
    }

    pub fn add(&self, key: K, value: V, policy: CacheItemPolicy) {
        tracing::debug!(
            name = %self.name,
            value = ?value,
            policy = ?policy,
            "added value to cache"
        );
        self.cache
            .insert(key, CacheItem::new(value, policy, self.clock.now_sec_u64()));
        self.drop_expired()
    }

    pub fn get(&self, key: &K) -> Option<V> {
        tracing::debug!(
            name = %self.name,
            key = ?key,
            "get value from cache"
        );
        self.cache.remove_if(key, |_, item| !self.is_valid(item));
        self.cache.get(key).map(|item| item.value.clone())
    }

    pub fn drop_expired(&self) {
        tracing::debug!(
            name = %self.name,
            "discarding expired items (if any)"
        );
        self.cache.retain(|_, item| self.is_valid(item));
    }

    fn is_valid(&self, item: &CacheItem<V>) -> bool {
        match item.policy {
            CacheItemPolicy::NoExpiration => true,
            CacheItemPolicy::AbsoluteExpiration(duration) => {
                let (created, duration, now) =
                    (item.created, duration.as_secs(), self.clock.now_sec_u64());
                created + duration > now
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::{CacheItemPolicy, MemoryCacheBase};
    use nekoton_utils::SimpleClock;
    use std::time::Duration;
    use tokio::time::sleep;

    #[test]
    pub fn test_non_expiring_caching() {
        let cache = MemoryCacheBase::with_clock("test", SimpleClock);

        let key1 = "key1".to_string();

        // returns nothing
        let value = cache.get(&key1);
        assert_eq!(value, None);

        cache.add(
            key1.clone(),
            "some_value".to_string(),
            CacheItemPolicy::NoExpiration,
        );

        // returns added value from now on...
        let value = cache.get(&key1);
        assert_eq!(value, Some("some_value".to_string()));

        let value = cache.get(&key1);
        assert_eq!(value, Some("some_value".to_string()));
    }

    #[tokio::test]
    async fn test_caching_with_absolute_expiration() {
        let cache = MemoryCacheBase::with_clock("test", SimpleClock);

        let key1 = "key1".to_string();

        // returns nothing
        let value = cache.get(&key1);
        assert_eq!(value, None);

        cache.add(
            key1.clone(),
            "some_value".to_string(),
            CacheItemPolicy::AbsoluteExpiration(Duration::from_secs(1)),
        );

        // returns value added
        let value = cache.get(&key1);
        assert_eq!(value, Some("some_value".to_string()));

        sleep(Duration::from_secs_f32(1.1)).await;

        // returns nothing again
        let value = cache.get(&key1);
        assert_eq!(value, None);
    }

    #[test]
    pub fn test_can_rewrite_cache() {
        let cache = MemoryCacheBase::with_clock("test", SimpleClock);

        let key1 = "key1".to_string();

        cache.add(
            key1.clone(),
            "some_value".to_string(),
            CacheItemPolicy::AbsoluteExpiration(Duration::from_secs(60)),
        );

        cache.add(
            key1,
            "another_value".to_string(),
            CacheItemPolicy::NoExpiration,
        );
    }
}
