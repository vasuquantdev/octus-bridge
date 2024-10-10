use std::sync::Arc;

use crate::utils::{CacheItemPolicy, MemoryCache};
use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct TokenInfo {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

#[derive(Clone)]
pub struct TokenMetaClient {
    rq_client: reqwest::Client,
    base_url: String,
    meta_cache: Arc<MemoryCache<String, TokenInfo>>,
}

impl TokenMetaClient {
    pub fn new(base_url: &str) -> Self {
        let rq_client = reqwest::Client::new();
        Self {
            rq_client,
            base_url: base_url.to_owned(),
            meta_cache: Arc::new(MemoryCache::new("token meta")),
        }
    }

    pub async fn get_token_meta(&self, address: &str) -> Result<TokenInfo> {
        crate::from_cache_or_recache!(
            &self.meta_cache,
            address.to_string(),
            {
                tracing::debug!(address = ?address, "getting token meta");

                let res = self
                    .rq_client
                    .get(format!("{}/token/{address}", self.base_url))
                    .send()
                    .await
                    .context("failed to get token meta")?;

                res.json::<TokenInfo>()
                    .await
                    .context("token meta response parsing failed")?
            },
            CacheItemPolicy::NoExpiration
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::ton_meta::tokens::{TokenInfo, TokenMetaClient};

    #[tokio::test]
    async fn test_api() {
        let client = TokenMetaClient::new("https://ton-tokens-api.bf.works");
        let meta = client
            .get_token_meta("0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe")
            .await
            .unwrap();

        assert_eq!(
            meta,
            TokenInfo {
                name: "Tether USD".to_string(),
                symbol: "USD₮".to_string(),
                decimals: 6
            }
        );

        let meta2 = client
            .get_token_meta("EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs")
            .await
            .unwrap();

        assert_eq!(meta, meta2);
    }
}
