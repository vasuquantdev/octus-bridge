use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub addr: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: "https://main.ton.dev/graphql".to_owned(),
        }
    }
}
