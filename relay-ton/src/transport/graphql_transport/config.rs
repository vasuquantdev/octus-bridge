use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub addr: String,
    pub next_block_timeout_sec: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: "https://main.ton.dev/graphql".to_owned(),
            next_block_timeout_sec: 60,
        }
    }
}
