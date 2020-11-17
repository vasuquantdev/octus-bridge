pub mod config;

use ton_abi::Function;
use ton_block::{Deserializable, MsgAddressInt};
use tonlib::{TonlibClient, TonlibError};

use self::config::*;
use super::errors::*;
use super::Transport;
use crate::models::*;

pub struct TonlibTransport {
    client: TonlibClient,
}

impl TonlibTransport {
    pub async fn new(config: Config) -> TransportResult<Self> {
        let client = tonlib::TonlibClient::new(config.into())
            .await
            .map_err(to_api_failure)?;

        Ok(Self { client })
    }
}

#[async_trait]
impl Transport for TonlibTransport {
    async fn get_account_state(
        &self,
        addr: &MsgAddressInt,
    ) -> TransportResult<Option<AccountState>> {
        let address =
            tonlib::utils::make_address_from_str(&addr.to_string()).map_err(to_api_failure)?;

        let account_state = self
            .client
            .get_account_state(address)
            .await
            .map_err(to_api_failure)?;

        let account_stuff =
            ton_block::AccountStuff::construct_from_bytes(account_state.data.0.as_slice())
                .map_err(|e| TransportError::FailedToParseAccountState {
                    reason: e.to_string(),
                })?;

        Ok(Some(AccountState {
            balance: account_stuff.storage.balance.grams.0.into(),
            last_transaction: Some(account_state.last_trans_lt as u64),
        }))
    }

    async fn send_message(
        &self,
        abi: &Function,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        unimplemented!()
    }
}

fn to_api_failure(e: TonlibError) -> TransportError {
    TransportError::ApiFailure {
        reason: e.as_fail().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const MAINNET_CONFIG: &str = r#"{
      "liteservers": [
        {
          "ip": 916349379,
          "port": 3031,
          "id": {
            "@type": "pub.ed25519",
            "key": "uNRRL+6enQjuiZ/s6Z+vO7yxUUR7uxdfzIy+RxkECrc="
          }
        }
      ],
      "validator": {
        "@type": "validator.config.global",
        "zero_state": {
          "workchain": -1,
          "shard": -9223372036854775808,
          "seqno": 0,
          "root_hash": "WP/KGheNr/cF3lQhblQzyb0ufYUAcNM004mXhHq56EU=",
          "file_hash": "0nC4eylStbp9qnCq8KjDYb789NjS25L5ZA1UQwcIOOQ="
        }
      }
    }"#;

    fn elector_addr() -> MsgAddressInt {
        MsgAddressInt::from_str(
            "-1:3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap()
    }

    async fn make_transport() -> TonlibTransport {
        TonlibTransport::new(Config {
            network_config: MAINNET_CONFIG.to_string(),
            network_name: "mainnet".to_string(),
            verbosity: 4,
            keystore: KeystoreType::InMemory,
            last_block_threshold_sec: 1,
        })
        .await
        .unwrap()
    }

    #[test]
    fn test_init() {
        let transport = make_transport().await;

        let account_state = transport.get_account_state(&elector_addr()).await.unwrap();
        println!("Account state: {:?}", account_state);
    }
}
