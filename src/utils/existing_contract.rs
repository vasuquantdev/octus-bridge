use anyhow::Result;
use nekoton_abi::{ExecutionOutput, FunctionExt, GenTimings, LastTransactionId, TransactionId};
use nekoton_utils::NoFailure;

pub struct ExistingContract {
    pub account: ton_block::AccountStuff,
    pub last_transaction_id: LastTransactionId,
}

impl ExistingContract {
    pub fn from_shard_account(
        shard_account: &Option<ton_block::ShardAccount>,
    ) -> Result<Option<Self>> {
        let shard_account = match shard_account {
            Some(shard_account) => shard_account,
            None => return Ok(None),
        };

        Ok(match shard_account.read_account().convert()? {
            ton_block::Account::Account(account) => Some(Self {
                account,
                last_transaction_id: LastTransactionId::Exact(TransactionId {
                    lt: shard_account.last_trans_lt(),
                    hash: *shard_account.last_trans_hash(),
                }),
            }),
            ton_block::Account::AccountNone => None,
        })
    }

    pub fn run_local(
        &self,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<Vec<ton_abi::Token>> {
        let ExecutionOutput {
            tokens,
            result_code,
        } = function.run_local(
            self.account.clone(),
            GenTimings::Unknown,
            &self.last_transaction_id,
            input,
        )?;

        tokens.ok_or_else(|| ExistingContractError::NonZeroResultCode(result_code).into())
    }
}

#[derive(thiserror::Error, Debug)]
enum ExistingContractError {
    #[error("Non zero result code: {}", .0)]
    NonZeroResultCode(i32),
}
