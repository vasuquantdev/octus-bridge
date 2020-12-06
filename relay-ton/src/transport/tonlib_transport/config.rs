use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub server_address: SocketAddr,
    pub server_key: String,
    pub last_block_threshold_sec: u64,
    pub subscription_polling_interval_sec: u64,

    /// seconds
    #[serde(default)]
    pub max_initial_rescan_gap: Option<u32>,
    /// seconds
    #[serde(default)]
    pub max_rescan_gap: Option<u32>,
}

impl Config {
    pub fn tonlib_config(&self) -> tonlib::Config {
        tonlib::Config {
            server_address: self.server_address,
            server_key: self.server_key.clone(),
            last_block_threshold: Duration::from_secs(self.last_block_threshold_sec),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        default_testnet_config()
    }
}

pub fn default_mainnet_config() -> Config {
    Config {
        server_address: SocketAddrV4::new(Ipv4Addr::new(54, 158, 97, 195), 3031).into(),
        server_key: "uNRRL+6enQjuiZ/s6Z+vO7yxUUR7uxdfzIy+RxkECrc=".to_owned(),
        last_block_threshold_sec: 1,
        subscription_polling_interval_sec: 1,
        max_initial_rescan_gap: None,
        max_rescan_gap: None,
    }
}

pub fn default_testnet_config() -> Config {
    Config {
        server_address: SocketAddrV4::new(Ipv4Addr::new(54, 158, 97, 195), 3032).into(),
        server_key: "uNRRL+6enQjuiZ/s6Z+vO7yxUUR7uxdfzIy+RxkECrc=".to_owned(),
        last_block_threshold_sec: 1,
        subscription_polling_interval_sec: 1,
        max_initial_rescan_gap: None,
        max_rescan_gap: None,
    }
}
