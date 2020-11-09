use crate::prelude::*;

#[derive(Debug, Clone, Deserialize)]
pub struct ClientConfig {
    pub server_address: String,
    pub network_retries_count: i8,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_address: Default::default(),
            network_retries_count: default_network_retries_count(),
        }
    }
}

pub fn default_network_retries_count() -> i8 {
    5
}
