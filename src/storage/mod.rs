use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use nekoton_abi::{LastTransactionId, TransactionId};
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use ton_block::{Deserializable, HashmapAugType};
use ton_indexer::utils::{RefMcStateHandle, ShardStateStuff};
use ton_types::{HashmapType, UInt256};
use weedb::{rocksdb, Caches, Migrations, Semver, Table, WeeDb};

use crate::utils::{contains_account, ExistingContract};

pub mod tables;

#[derive(Default)]
pub struct RuntimeStorage {
    shard_accounts_cache: RwLock<FxHashMap<ton_block::ShardIdent, ShardAccounts>>,
}

impl RuntimeStorage {
    pub fn update_contract_states(
        &self,
        block_id: &ton_block::BlockIdExt,
        block_info: &ton_block::BlockInfo,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        let accounts = shard_state.state().read_accounts()?;
        let state_handle = shard_state.ref_mc_state_handle().clone();

        let shard_accounts = ShardAccounts {
            accounts,
            state_handle,
        };

        if !block_id.shard_id.is_masterchain() {
            let mut cache = self.shard_accounts_cache.write();

            cache.insert(*block_info.shard(), shard_accounts);
            if block_info.after_merge() || block_info.after_split() {
                tracing::debug!("Clearing shard states cache after shards merge/split");

                let block_ids = block_info.read_prev_ids()?;
                match block_ids.len() {
                    // Block after split
                    //       |
                    //       *  - block A
                    //      / \
                    //     *   *  - blocks B', B"
                    1 => {
                        // Find all split shards for the block A
                        let (left, right) = block_ids[0].shard_id.split()?;

                        // Remove parent shard of the block A
                        if cache.contains_key(&left) && cache.contains_key(&right) {
                            cache.remove(&block_ids[0].shard_id);
                        }
                    }

                    // Block after merge
                    //     *   *  - blocks A', A"
                    //      \ /
                    //       *  - block B
                    //       |
                    2 => {
                        // Find and remove all parent shards
                        for block_id in block_info.read_prev_ids()? {
                            cache.remove(&block_id.shard_id);
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    pub fn get_contract_state(&self, account: &ton_types::UInt256) -> Result<Option<ShardAccount>> {
        let cache = self.shard_accounts_cache.read();
        let mut state = Ok(None);

        for (shard_ident, shard_accounts) in cache.iter() {
            if !contains_account(shard_ident, account) {
                continue;
            }

            state = shard_accounts.get(account)
        }

        state
    }
}

pub struct ShardAccount {
    pub data: ton_block::ShardAccount,
    pub state_handle: Arc<RefMcStateHandle>,
}

struct ShardAccounts {
    accounts: ton_block::ShardAccounts,
    state_handle: Arc<RefMcStateHandle>,
}

impl ShardAccounts {
    fn get(&self, account: &ton_types::UInt256) -> Result<Option<ShardAccount>> {
        match self.accounts.get(account)? {
            Some(account) => Ok(Some(ShardAccount {
                data: account,
                state_handle: self.state_handle.clone(),
            })),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DbOptions {
    pub max_memory_usage: usize,
    pub min_caches_capacity: usize,
    pub min_compaction_memory_budget: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            max_memory_usage: 2 << 30,             // 2 GB
            min_caches_capacity: 64 << 20,         // 64 MB
            min_compaction_memory_budget: 1 << 30, // 1 GB
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistentStorageConfig {
    pub persistent_db_path: PathBuf,
    #[serde(default)]
    pub persistent_db_options: DbOptions,
    #[serde(default)]
    pub shard_split_depth: u8,
}

pub struct PersistentStorage {
    pub code_hashes: Table<tables::CodeHashes>,
    pub code_hashes_by_address: Table<tables::CodeHashesByAddress>,

    pub snapshot: ArcSwapOption<OwnedSnapshot>,
    pub inner: WeeDb,

    pub shard_split_depth: u8,
}

impl PersistentStorage {
    const DB_VERSION: Semver = [0, 1, 0];

    pub fn new(config: &PersistentStorageConfig) -> Result<Self> {
        let limit = match fdlimit::raise_fd_limit() {
            // New fd limit
            Some(limit) => limit,
            // Current soft limit
            None => {
                rlimit::getrlimit(rlimit::Resource::NOFILE)
                    .unwrap_or((256, 0))
                    .0
            }
        };
        let options = &config.persistent_db_options;

        let caches_capacity =
            std::cmp::max(options.max_memory_usage / 3, options.min_caches_capacity);
        let compaction_memory_budget = std::cmp::max(
            options.max_memory_usage - options.max_memory_usage / 3,
            options.min_compaction_memory_budget,
        );

        let caches = Caches::with_capacity(caches_capacity);

        let inner = WeeDb::builder(&config.persistent_db_path, caches)
            .options(|opts, _| {
                opts.set_level_compaction_dynamic_level_bytes(true);

                // compression opts
                opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

                // io
                opts.set_max_open_files(limit as i32);

                // logging
                opts.set_log_level(rocksdb::LogLevel::Error);
                opts.set_keep_log_file_num(2);
                opts.set_recycle_log_file_num(2);

                // cf
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);

                // cpu
                opts.set_max_background_jobs(std::cmp::max((num_cpus::get() as i32) / 2, 2));
                opts.increase_parallelism(num_cpus::get() as i32);

                opts.optimize_level_style_compaction(compaction_memory_budget);

                // debug
                // opts.enable_statistics();
                // opts.set_stats_dump_period_sec(30);
            })
            .with_table::<tables::CodeHashes>()
            .with_table::<tables::CodeHashesByAddress>()
            .build()
            .context("Failed building db")?;

        let migrations = Migrations::with_target_version(Self::DB_VERSION);
        inner
            .apply(migrations)
            .context("Failed to apply migrations")?;

        Ok(Self {
            code_hashes: inner.instantiate_table(),
            code_hashes_by_address: inner.instantiate_table(),
            snapshot: Default::default(),
            inner,
            shard_split_depth: config.shard_split_depth,
        })
    }

    pub fn load_snapshot(&self) -> Option<Arc<OwnedSnapshot>> {
        self.snapshot.load_full()
    }

    pub fn update_snapshot(&self) {
        let snapshot = Arc::new(OwnedSnapshot::new(self.inner.raw().clone()));
        self.snapshot.store(Some(snapshot));
    }

    pub async fn reset_accounts(&self, shard_state: Arc<ShardStateStuff>) -> Result<()> {
        let shard = *shard_state.shard();
        let workchain = shard.workchain_id();
        let Ok(workchain) = i8::try_from(workchain) else {
            return Ok(());
        };

        let now = Instant::now();
        tracing::info!(%shard, "clearing old code hash indices");
        self.remove_code_hashes(&shard).await?;
        tracing::info!(
            %shard,
            elapsed = %humantime::format_duration(now.elapsed()),
            "cleared old code hash indices",
        );

        // Split on virtual shards
        let (_state_guard, virtual_shards) = {
            let guard = shard_state.ref_mc_state_handle().clone();

            let mut virtual_shards = FxHashMap::default();
            split_shard(
                shard,
                shard_state.state().read_accounts()?,
                self.shard_split_depth,
                &mut virtual_shards,
            )
            .context("Failed to split shard state into virtual shards")?;

            // NOTE: ensure that root cell is dropped
            drop(shard_state);
            (guard, virtual_shards)
        };

        // Prepare column families
        let mut write_batch = rocksdb::WriteBatch::default();
        let db = self.inner.raw().as_ref();
        let code_hashes_cf = &self.code_hashes.cf();
        let code_hashes_by_address_cf = &self.code_hashes_by_address.cf();

        // Prepare buffer for code hashes ids
        let mut code_hashes_id = [0u8; { tables::CodeHashes::KEY_LEN }];
        code_hashes_id[32] = workchain as u8;

        let mut code_hashes_by_address_id = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
        code_hashes_by_address_id[0] = workchain as u8;

        // Iterate all changed accounts in block
        let mut non_empty_batch = false;
        let _now = Instant::now();
        tracing::info!(%shard, "building new code hash indices");

        for (virtual_shard, accounts) in virtual_shards {
            let now = Instant::now();
            tracing::info!(%shard, %virtual_shard, "collecting code hashes");

            for entry in accounts.iter() {
                let (id, mut account) = entry?;
                let id: &[u8; 32] = match id.data().try_into() {
                    Ok(data) => data,
                    Err(_) => continue,
                };

                let code_hash = {
                    ton_block::DepthBalanceInfo::construct_from(&mut account)?; // skip an augmentation
                    match extract_code_hash(ton_block::ShardAccount::construct_from(&mut account)?)?
                    {
                        Some(code_hash) => code_hash,
                        None => continue,
                    }
                };

                non_empty_batch |= true;

                // Fill account address in full code hashes buffer
                code_hashes_id[..32].copy_from_slice(code_hash.as_slice());
                code_hashes_id[33..65].copy_from_slice(id);

                code_hashes_by_address_id[1..33].copy_from_slice(id);

                // Write tx data and indices
                write_batch.put_cf(code_hashes_cf, code_hashes_id.as_slice(), []);
                write_batch.put_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                    code_hash.as_slice(),
                );
            }

            tracing::info!(
                %shard,
                %virtual_shard,
                elapsed = %humantime::format_duration(now.elapsed()),
                "collected code hashes",
            );
        }

        if non_empty_batch {
            db.write_opt(write_batch, self.code_hashes.write_config())
                .context("Failed to update server storage")?;
        }
        tracing::info!(
            %shard,
            elapsed = %humantime::format_duration(now.elapsed()),
            "built new code hash indices",
        );

        // Flush indices after delete/insert
        let _now = Instant::now();
        tracing::info!(%shard, "flushing code hash indices");
        let bound = Option::<[u8; 0]>::None;
        db.compact_range_cf(code_hashes_cf, bound, bound);
        db.compact_range_cf(code_hashes_by_address_cf, bound, bound);
        tracing::info!(
            %shard,
            elapsed = %humantime::format_duration(now.elapsed()),
            "flushed code hash indices",
        );

        // Done
        Ok(())
    }

    pub fn update(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        let workchain = block_id.shard().workchain_id();
        let Ok(workchain) = i8::try_from(workchain) else {
            return Ok(());
        };

        let extra = block.read_extra()?;
        let account_blocks = extra.read_account_blocks()?;
        let accounts = shard_state
            .map(|shard_state| shard_state.state().read_accounts())
            .transpose()?;

        //

        // Prepare column families
        let mut write_batch = rocksdb::WriteBatch::default();

        // Iterate all changed accounts in block
        let mut non_empty_batch = false;
        account_blocks.iterate_with_keys(|account, value| {
            // Flag to update code hash
            let mut has_special_actions = accounts.is_none(); // skip updates for this flag if no state
            let mut was_active = false;
            let mut is_active = false;

            // Process account transactions
            let mut first_tx = true;
            value.transactions().iterate_slices(|_, mut value| {
                let tx_cell = value.checked_drain_reference()?;
                let tx = ton_block::Transaction::construct_from_cell(tx_cell)?;

                // Update marker flags
                if first_tx {
                    // Remember the original status from the first transaction
                    was_active = tx.orig_status == ton_block::AccountStatus::AccStateActive;
                    first_tx = false;
                }
                if was_active && tx.orig_status != ton_block::AccountStatus::AccStateActive {
                    // Handle the case when an account (with some updated code) was deleted,
                    // and then deployed with the initial code (end status).
                    // Treat this situation as a special action.
                    has_special_actions = true;
                }
                is_active = tx.end_status == ton_block::AccountStatus::AccStateActive;

                if !has_special_actions {
                    // Search for special actions (might be code hash update)
                    let descr = tx.read_description()?;
                    if let Some(action_phase) = descr.action_phase_ref() {
                        has_special_actions |= action_phase.spec_actions != 0;
                    }
                }

                Ok(true)
            })?;

            // Update code hash
            if let Some(accounts) = &accounts {
                let update = if is_active && (!was_active || has_special_actions) {
                    // Account is active after this block and this is either a new account,
                    // or it was an existing account which possibly changed its code.
                    // Update: just store the code hash.
                    Some(false)
                } else if was_active && !is_active {
                    // Account was active before this block and is not active after the block.
                    // Update: remove the code hash.
                    Some(true)
                } else {
                    // No update for other cases
                    None
                };

                // Apply the update if any
                if let Some(remove) = update {
                    non_empty_batch |= true;
                    self.update_code_hash(workchain, &account, accounts, remove, &mut write_batch)?;
                }
            }

            Ok(true)
        })?;

        if non_empty_batch {
            self.inner
                .raw()
                .write_opt(write_batch, self.code_hashes.write_config())
                .context("Failed to update server storage")?;
        }

        Ok(())
    }

    fn update_code_hash(
        &self,
        workchain: i8,
        account: &UInt256,
        accounts: &ton_block::ShardAccounts,
        remove: bool,
        write_batch: &mut rocksdb::WriteBatch,
    ) -> Result<()> {
        // Prepare column families
        let code_hashes_cf = &self.code_hashes.cf();
        let code_hashes_by_address_cf = &self.code_hashes_by_address.cf();

        // Check the secondary index first
        let mut code_hashes_by_address_id = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
        code_hashes_by_address_id[0] = workchain as u8;
        code_hashes_by_address_id[1..33].copy_from_slice(account.as_slice());

        // Find the old code hash
        let old_code_hash = self
            .code_hashes_by_address
            .get(code_hashes_by_address_id.as_slice())?;

        // Find the new code hash
        let new_code_hash = 'code_hash: {
            if !remove {
                if let Some(account) = accounts.get(account)? {
                    break 'code_hash extract_code_hash(account)?;
                }
            }
            None
        };

        if remove && old_code_hash.is_none()
            || matches!(
                (&old_code_hash, &new_code_hash),
                (Some(old), Some(new)) if old.as_ref() == new.as_slice()
            )
        {
            // Code hash should not be changed.
            return Ok(());
        }

        let mut code_hashes_id = [0u8; { tables::CodeHashes::KEY_LEN }];
        code_hashes_id[32] = workchain as u8;
        code_hashes_id[33..65].copy_from_slice(account.as_slice());

        // Remove entry from the primary index
        if let Some(old_code_hash) = old_code_hash {
            code_hashes_id[..32].copy_from_slice(&old_code_hash);
            write_batch.delete_cf(code_hashes_cf, code_hashes_id.as_slice());
        }

        match new_code_hash {
            Some(new_code_hash) => {
                // Update primary index
                code_hashes_id[..32].copy_from_slice(new_code_hash.as_slice());
                write_batch.put_cf(
                    code_hashes_cf,
                    code_hashes_id.as_slice(),
                    new_code_hash.as_slice(),
                );

                // Update secondary index
                write_batch.put_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                    new_code_hash.as_slice(),
                );
            }
            None => {
                // Remove entry from the secondary index
                write_batch.delete_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                );
            }
        }

        Ok(())
    }

    async fn remove_code_hashes(&self, shard: &ton_block::ShardIdent) -> Result<()> {
        let workchain = shard.workchain_id() as u8;

        // Remove from the secondary index first
        {
            let mut from = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
            from[0] = workchain;

            {
                let [_, from @ ..] = &mut from;
                extend_account_prefix(shard, false, from);
            }

            let mut to = from;
            {
                let [_, to @ ..] = &mut to;
                extend_account_prefix(shard, true, to);
            }

            let db = self.inner.raw().as_ref();
            let cf = &self.code_hashes_by_address.cf();
            let writeopts = self.code_hashes_by_address.write_config();

            // Remove `[from; to)`
            db.delete_range_cf_opt(cf, &from, &to, writeopts)?;
            // Remove `to`, (-1:ffff..ffff might be a valid existing address)
            db.delete_cf_opt(cf, to, writeopts)?;
        }

        // Full scan the main code hashes index and remove all entires for the shard
        let db = self.inner.raw().clone();
        let cf = self.code_hashes.get_unbounded_cf();
        let writeopts = self.code_hashes.new_write_config();
        let mut readopts = self.code_hashes.new_read_config();

        let shard = *shard;
        tokio::task::spawn_blocking(move || {
            let cf = cf.bound();
            let snapshot = db.snapshot();
            readopts.set_snapshot(&snapshot);

            let mut iter = db.raw_iterator_cf_opt(&cf, readopts);
            iter.seek_to_first();

            let mut prefix = shard.shard_prefix_with_tag();
            let mut lower_bits = ton_block::ShardIdent::lower_bits(prefix);
            prefix -= lower_bits;
            lower_bits |= lower_bits - 1;

            loop {
                let key = match iter.key() {
                    Some(key) => key,
                    None => return iter.status(),
                };

                if key.len() != tables::CodeHashes::KEY_LEN
                    || key[32] == workchain
                        && (shard.is_full() || {
                            let key = u64::from_be_bytes(key[33..41].try_into().unwrap());
                            (key ^ prefix) & !lower_bits == 0
                        })
                {
                    db.delete_cf_opt(&cf, key, &writeopts)?;
                }

                iter.next();
            }
        })
        .await??;

        // Done
        Ok(())
    }
}

impl Drop for PersistentStorage {
    fn drop(&mut self) {
        self.snapshot.store(None);
        self.inner.raw().cancel_all_background_work(true);
    }
}

pub struct OwnedSnapshot {
    inner: rocksdb::Snapshot<'static>,
    _db: Arc<rocksdb::DB>,
}

impl OwnedSnapshot {
    fn new(db: Arc<rocksdb::DB>) -> Self {
        use rocksdb::Snapshot;

        unsafe fn extend_lifetime<'a>(r: Snapshot<'a>) -> Snapshot<'static> {
            std::mem::transmute::<Snapshot<'a>, Snapshot<'static>>(r)
        }

        // SAFETY: `Snapshot` requires the same lifetime as `rocksdb::DB` but
        // `tokio::task::spawn` requires 'static. This object ensures
        // that `rocksdb::DB` object lifetime will exceed the lifetime of the snapshot
        let inner = unsafe { extend_lifetime(db.as_ref().snapshot()) };
        Self { inner, _db: db }
    }
}

impl std::ops::Deref for OwnedSnapshot {
    type Target = rocksdb::Snapshot<'static>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

fn extend_account_prefix(shard: &ton_block::ShardIdent, max: bool, target: &mut [u8; 32]) {
    let mut prefix = shard.shard_prefix_with_tag();
    if max {
        prefix |= prefix - 1;
    } else {
        prefix -= ton_block::ShardIdent::lower_bits(prefix);
    };
    target[..8].copy_from_slice(&prefix.to_be_bytes());
    target[8..].fill(0xff * max as u8);
}

fn extract_code_hash(account: ton_block::ShardAccount) -> Result<Option<ton_types::UInt256>> {
    if let ton_block::Account::Account(account) = account.read_account()? {
        if let ton_block::AccountState::AccountActive { state_init } = account.storage.state {
            if let Some(code) = state_init.code {
                return Ok(Some(code.repr_hash()));
            }
        }
    }
    Ok(None)
}

fn split_shard(
    ident: ton_block::ShardIdent,
    accounts: ton_block::ShardAccounts,
    depth: u8,
    shards: &mut FxHashMap<ton_block::ShardIdent, ton_block::ShardAccounts>,
) -> Result<()> {
    if depth == 0 {
        shards.insert(ident, accounts);
        return Ok(());
    }

    let (left_shard_ident, right_shard_ident) = ident.split()?;
    let (left_accounts, right_accounts) = accounts.split(&ident.shard_key(false))?;

    split_shard(left_shard_ident, left_accounts, depth - 1, shards)?;
    split_shard(right_shard_ident, right_accounts, depth - 1, shards)?;

    Ok(())
}

pub fn make_existing_contract(state: Option<ShardAccount>) -> Result<Option<ExistingContract>> {
    let state = match state {
        Some(this) => this,
        None => return Ok(None),
    };

    match state.data.read_account()? {
        ton_block::Account::AccountNone => Ok(None),
        ton_block::Account::Account(account) => Ok(Some(ExistingContract {
            account,
            last_transaction_id: LastTransactionId::Exact(TransactionId {
                lt: state.data.last_trans_lt(),
                hash: *state.data.last_trans_hash(),
            }),
        })),
    }
}
