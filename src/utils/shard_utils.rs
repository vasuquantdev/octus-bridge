use std::borrow::Borrow;
use std::collections::HashMap;

use anyhow::{Context, Result};
use tiny_adnl::utils::*;
use ton_block::{HashmapAugType, MsgAddrStd, MsgAddressInt};
use ton_types::{AccountId, UInt256};

use super::existing_contract::*;

pub type ShardsMap = HashMap<ton_block::ShardIdent, ton_block::BlockIdExt>;

#[derive(Debug, Clone)]
pub struct LatestShardBlocks {
    pub current_utime: u32,
    pub block_ids: ShardsMap,
}

pub type ShardAccountsMap = FxHashMap<ton_block::ShardIdent, ton_block::ShardAccounts>;

/// Helper trait to reduce boilerplate for getting accounts from shards state
pub trait ShardAccountsMapExt {
    /// Looks for a suitable shard and tries to extract information about the contract from it
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>>;
}

impl<T> ShardAccountsMapExt for &T
where
    T: ShardAccountsMapExt,
{
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>> {
        T::find_account(self, account)
    }
}

impl ShardAccountsMapExt for ShardAccountsMap {
    fn find_account(&self, account: &UInt256) -> Result<Option<ExistingContract>> {
        // Search suitable shard for account by prefix.
        // NOTE: In **most** cases suitable shard will be found
        let item = self
            .iter()
            .find(|(shard_ident, _)| contains_account(shard_ident, account));

        match item {
            // Search account in shard state
            Some((_, shard)) => match shard
                .get(account)
                .and_then(|account| ExistingContract::from_shard_account_opt(&account))?
            {
                // Account found
                Some(contract) => Ok(Some(contract)),
                // Account was not found (it never had any transactions) or there is not AccountStuff in it
                None => Ok(None),
            },
            // Exceptional situation when no suitable shard was found
            None => Err(ShardUtilsError::InvalidContractAddress).context("No suitable shard found"),
        }
    }
}

pub fn contains_account(shard: &ton_block::ShardIdent, account: &UInt256) -> bool {
    let shard_prefix = shard.shard_prefix_with_tag();
    if shard_prefix == ton_block::SHARD_FULL {
        true
    } else {
        let len = shard.prefix_len();
        let account_prefix = account_prefix(account, len as usize) >> (64 - len);
        let shard_prefix = shard_prefix >> (64 - len);
        account_prefix == shard_prefix
    }
}

pub fn account_prefix(account: &UInt256, len: usize) -> u64 {
    debug_assert!(len <= 64);

    let account = account.as_slice();

    let mut value: u64 = 0;

    let bytes = len / 8;
    for (i, byte) in account.iter().enumerate().take(bytes) {
        value |= (*byte as u64) << (8 * (7 - i));
    }

    let remainder = len % 8;
    if remainder > 0 {
        let r = account[bytes] >> (8 - remainder);
        value |= (r as u64) << (8 * (7 - bytes) + 8 - remainder);
    }

    value
}

pub fn only_account_hash<T>(address: T) -> UInt256
where
    T: Borrow<ton_block::MsgAddressInt>,
{
    UInt256::from_be_bytes(&address.borrow().address().get_bytestring(0))
}

#[derive(thiserror::Error, Debug)]
enum ShardUtilsError {
    #[error("Invalid contract address")]
    InvalidContractAddress,
}

pub fn account_to_address(account: UInt256) -> MsgAddressInt {
    MsgAddressInt::AddrStd(MsgAddrStd::with_address(
        None,
        0,
        AccountId::new(account.as_slice().to_vec()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_prefix() {
        let mut account_id = [0u8; 32];
        for byte in account_id.iter_mut().take(8) {
            *byte = 0xff;
        }

        let account_id = UInt256::from(account_id);
        for i in 0..64 {
            let prefix = account_prefix(&account_id, i);
            assert_eq!(64 - prefix.trailing_zeros(), i as u32);
        }
    }

    #[test]
    fn test_contains_account() {
        let account = ton_types::UInt256::from_be_bytes(
            &hex::decode("459b6795bf4d4c3b930c83fe7625cfee99a762e1e114c749b62bfa751b781fa5")
                .unwrap(),
        );

        let mut shards =
            vec![ton_block::ShardIdent::with_tagged_prefix(0, ton_block::SHARD_FULL).unwrap()];
        for _ in 0..4 {
            let mut new_shards = vec![];
            for shard in &shards {
                let (left, right) = shard.split().unwrap();
                new_shards.push(left);
                new_shards.push(right);
            }

            shards = new_shards;
        }

        let mut target_shard = None;
        for shard in shards {
            if !contains_account(&shard, &account) {
                continue;
            }

            if target_shard.is_some() {
                panic!("Account can't be in two shards");
            }
            target_shard = Some(shard);
        }

        assert!(
            matches!(target_shard, Some(shard) if shard.shard_prefix_with_tag() == 0x4800000000000000)
        );
    }
}
