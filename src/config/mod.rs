use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};

use crate::storage::PersistentStorageConfig;
use anyhow::{Context, Result};
use everscale_network::{adnl, dht, overlay, rldp};
use nekoton_utils::*;
use rand::Rng;
use secstr::SecUtf8;
use serde::{Deserialize, Serialize};
use url::Url;

pub use self::eth_config::*;
pub use self::sol_config::*;
pub use self::stored_keys::*;
pub use self::verification_state::*;

mod eth_config;
mod sol_config;
mod stored_keys;
mod verification_state;

/// Main application config (full). Used to run relay
#[derive(Serialize, Deserialize)]
pub struct AppConfig {
    /// Password, used to encode and decode data in keystore
    pub master_password: SecUtf8,

    /// Staker address from which keys were submitted
    #[serde(with = "serde_address")]
    pub staker_address: ton_block::MsgAddressInt,

    /// Bridge related settings
    pub bridge_settings: BridgeConfig,

    /// TON node settings
    #[serde(default)]
    pub node_settings: NodeConfig,

    /// Persistent accounts storage settings
    pub storage: PersistentStorageConfig,

    /// Prometheus metrics exporter settings.
    /// Completely disable when not specified
    #[serde(default)]
    pub metrics_settings: Option<pomfrit::Config>,
}

/// Main application config (brief). Used for simple commands that require only password
#[derive(Serialize, Deserialize)]
pub struct BriefAppConfig {
    /// Password, used to encode and decode data in keystore
    #[serde(default)]
    pub master_password: Option<SecUtf8>,
}

/// Bridge related settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeConfig {
    /// Path to the file with keystore data
    pub keys_path: PathBuf,

    /// Bridge contract address
    #[serde(with = "serde_address")]
    pub bridge_address: ton_block::MsgAddressInt,

    /// If set, relay will not participate in elections. Default: false
    #[serde(default)]
    pub ignore_elections: bool,

    /// EVM networks settings
    pub evm_networks: Vec<EthConfig>,

    /// Solana network settings
    #[serde(default)]
    pub sol_network: Option<SolConfig>,

    /// ETH address verification settings
    #[serde(default)]
    pub address_verification: AddressVerificationConfig,

    /// Shard split depth. Default: 1
    #[serde(default)]
    pub shard_split_depth: u8,

    /// Ton token metadata endpoint base url
    #[cfg(feature = "ton")]
    pub token_meta_base_url: String,

    pub jrpc_endpoints: Vec<Url>,
}

/// ETH address verification settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AddressVerificationConfig {
    /// Minimal balance on user's wallet to start address verification
    /// Default: 50000000 (0.05 ETH)
    pub min_balance_gwei: u64,

    /// Fixed gas price. Default: 300
    pub gas_price_gwei: u64,

    /// Path to the file with transaction state.
    /// Default: `./verification-state.json`
    pub state_path: PathBuf,
}

impl Default for AddressVerificationConfig {
    fn default() -> Self {
        Self {
            min_balance_gwei: 50000000,
            gas_price_gwei: 300,
            state_path: "verification-state.json".into(),
        }
    }
}

/// TON node settings
#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// Node public ip. Automatically determines if None
    pub adnl_public_ip: Option<Ipv4Addr>,

    /// Node port. Default: 30303
    pub adnl_port: u16,

    /// Path to the DB directory. Default: `./db`
    pub db_path: PathBuf,

    /// Path to the ADNL keys. Default: `./adnl-keys.json`.
    /// NOTE: generates new keys if specified path doesn't exist
    pub temp_keys_path: PathBuf,

    /// Internal DB options.
    pub db_options: ton_indexer::DbOptions,

    /// Increases memory usage, but makes states processing faster. Default: enabled
    pub shard_state_cache_options: Option<ton_indexer::ShardStateCacheOptions>,

    /// Archives map queue. Default: 16
    pub parallel_archive_downloads: usize,

    /// Whether old shard states will be removed every 10 minutes
    pub states_gc_enabled: bool,

    /// Whether old blocks will be removed on each new key block
    pub blocks_gc_enabled: bool,

    pub adnl_options: adnl::NodeOptions,
    pub rldp_options: rldp::NodeOptions,
    pub dht_options: dht::NodeOptions,
    pub overlay_shard_options: overlay::OverlayOptions,
    pub neighbours_options: ton_indexer::NeighboursOptions,
}

impl NodeConfig {
    pub async fn build_indexer_config(mut self) -> Result<ton_indexer::NodeConfig> {
        // Determine public ip
        let ip_address = broxus_util::resolve_public_ip(self.adnl_public_ip).await?;
        tracing::info!(?ip_address, "using public ip");

        // Generate temp keys
        let adnl_keys = ton_indexer::NodeKeys::load(self.temp_keys_path, false)
            .context("Failed to load temp keys")?;

        // Prepare DB folder
        std::fs::create_dir_all(&self.db_path)?;

        self.rldp_options.force_compression = true;
        self.overlay_shard_options.force_compression = true;

        // Done
        Ok(ton_indexer::NodeConfig {
            ip_address: SocketAddrV4::new(ip_address, self.adnl_port),
            adnl_keys,
            rocks_db_path: self.db_path.join("rocksdb"),
            file_db_path: self.db_path.join("files"),
            state_gc_options: self.states_gc_enabled.then(|| ton_indexer::StateGcOptions {
                offset_sec: rand::thread_rng().gen_range(0..3600),
                interval_sec: 3600,
            }),
            blocks_gc_options: self
                .blocks_gc_enabled
                .then(|| ton_indexer::BlocksGcOptions {
                    kind: ton_indexer::BlocksGcKind::BeforePreviousKeyBlock,
                    enable_for_sync: true,
                    ..Default::default()
                }),
            persistent_state_options: Default::default(),
            shard_state_cache_options: None, // self.shard_state_cache_options,
            db_options: self.db_options,
            archive_options: Some(Default::default()),
            sync_options: ton_indexer::SyncOptions {
                parallel_archive_downloads: self.parallel_archive_downloads,
                ..Default::default()
            },
            adnl_options: self.adnl_options,
            rldp_options: self.rldp_options,
            dht_options: self.dht_options,
            neighbours_options: self.neighbours_options,
            overlay_shard_options: self.overlay_shard_options,
        })
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            adnl_public_ip: None,
            adnl_port: 30303,
            db_path: "db".into(),
            temp_keys_path: "adnl-keys.json".into(),
            shard_state_cache_options: Some(Default::default()),
            parallel_archive_downloads: 16,
            states_gc_enabled: true,
            blocks_gc_enabled: true,
            db_options: Default::default(),
            adnl_options: Default::default(),
            rldp_options: Default::default(),
            dht_options: Default::default(),
            overlay_shard_options: Default::default(),
            neighbours_options: Default::default(),
        }
    }
}

impl ConfigExt for ton_indexer::GlobalConfig {
    fn from_file<P>(path: &P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}

pub trait ConfigExt: Sized {
    fn from_file<P>(path: &P) -> Result<Self>
    where
        P: AsRef<Path>;
}
