use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub server_address: SocketAddrV4,
    pub server_key: String,

    #[serde(default = "default_max_connection_count")]
    pub max_connection_count: u32,

    #[serde(default = "default_min_idle_connection_count")]
    pub min_idle_connection_count: Option<u32>,

    #[serde(default = "default_socket_timeout", with = "relay_utils::serde_time")]
    pub socket_read_timeout: Duration,

    #[serde(default = "default_socket_timeout", with = "relay_utils::serde_time")]
    pub socket_send_timeout: Duration,

    #[serde(default = "default_ping_timeout", with = "relay_utils::serde_time")]
    pub ping_timeout: Duration,

    /// Latest block id cache duration
    #[serde(with = "relay_utils::serde_time")]
    pub last_block_threshold: Duration,

    /// Account state polling interval
    #[serde(with = "relay_utils::serde_time")]
    pub subscription_polling_interval: Duration,

    #[serde(default, with = "relay_utils::optional_serde_time")]
    pub max_initial_rescan_gap: Option<Duration>,

    #[serde(default, with = "relay_utils::optional_serde_time")]
    pub max_rescan_gap: Option<Duration>,
}

fn default_max_connection_count() -> u32 {
    10
}

fn default_min_idle_connection_count() -> Option<u32> {
    Some(5)
}

fn default_socket_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_ping_timeout() -> Duration {
    Duration::from_secs(10)
}

impl Config {
    pub fn tonlib_config(&self) -> tonlib::Config {
        tonlib::Config {
            server_address: self.server_address,
            server_key: self.server_key.clone(),
            max_connection_count: self.max_connection_count,
            min_idle_connection_count: self.min_idle_connection_count,
            socket_read_timeout: self.socket_read_timeout,
            socket_send_timeout: self.socket_send_timeout,
            last_block_threshold: self.last_block_threshold,
            ping_timeout: self.ping_timeout,
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
        server_address: SocketAddrV4::new(Ipv4Addr::new(54, 158, 97, 195), 3031),
        server_key: "uNRRL+6enQjuiZ/s6Z+vO7yxUUR7uxdfzIy+RxkECrc=".to_owned(),
        last_block_threshold: Duration::from_secs(1),
        max_connection_count: default_max_connection_count(),
        min_idle_connection_count: default_min_idle_connection_count(),
        socket_read_timeout: default_socket_timeout(),
        socket_send_timeout: default_socket_timeout(),
        ping_timeout: default_ping_timeout(),
        subscription_polling_interval: Duration::from_secs(1),
        max_initial_rescan_gap: None,
        max_rescan_gap: None,
    }
}

pub fn default_testnet_config() -> Config {
    Config {
        server_address: SocketAddrV4::new(Ipv4Addr::new(54, 158, 97, 195), 3032),
        server_key: "uNRRL+6enQjuiZ/s6Z+vO7yxUUR7uxdfzIy+RxkECrc=".to_owned(),
        last_block_threshold: Duration::from_secs(1),
        max_connection_count: default_max_connection_count(),
        min_idle_connection_count: default_min_idle_connection_count(),
        socket_read_timeout: default_socket_timeout(),
        socket_send_timeout: default_socket_timeout(),
        ping_timeout: default_ping_timeout(),
        subscription_polling_interval: Duration::from_secs(1),
        max_initial_rescan_gap: None,
        max_rescan_gap: None,
    }
}
