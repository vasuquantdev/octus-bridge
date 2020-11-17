pub mod config;
mod tvm;

use failure::AsFail;
use ton_abi::Function;
use ton_api::ton;
use ton_block::{
    AccountStuff, CommonMsgInfo, Deserializable, ExternalInboundMessageHeader, Message, MsgAddrVar,
    MsgAddressInt,
};
use ton_types::SliceData;
use tonlib::{TonlibClient, TonlibError};

use self::config::*;
use super::errors::*;
use super::Transport;
use crate::models::*;
use crate::prelude::*;

pub struct TonlibTransport {
    client: TonlibClient,
}

impl TonlibTransport {
    pub async fn new(config: Config) -> TransportResult<Self> {
        let client = tonlib::TonlibClient::new(&config.into())
            .await
            .map_err(to_api_error)?;

        Ok(Self { client })
    }

    async fn run_local(
        &self,
        function: &AbiFunction,
        address: &MsgAddressInt,
        message: &Message,
    ) -> TransportResult<ContractOutput> {
        let address =
            tonlib::utils::make_address_from_str(&address.to_string()).map_err(to_api_error)?;

        let account_state = self
            .client
            .get_account_state(address)
            .await
            .map_err(to_api_error)?;

        let info = parse_account_stuff(account_state.data.0.as_slice())?;

        let (messages, _) = tvm::call_msg(&account_state, info, message)?;

        process_out_messages(&messages, function)
    }
}

#[async_trait]
impl Transport for TonlibTransport {
    async fn get_account_state(
        &self,
        addr: &MsgAddressInt,
    ) -> TransportResult<Option<AccountState>> {
        let address =
            tonlib::utils::make_address_from_str(&addr.to_string()).map_err(to_api_error)?;

        let account_state = self
            .client
            .get_account_state(address)
            .await
            .map_err(to_api_error)?;

        let info = parse_account_stuff(account_state.data.0.as_slice())?;

        Ok(Some(AccountState {
            balance: info.storage.balance.grams.0.into(),
            last_transaction: Some(account_state.last_trans_lt as u64),
        }))
    }

    async fn send_message(
        &self,
        abi: &Function,
        message: ExternalMessage,
    ) -> TransportResult<ContractOutput> {
        let mut message_header = ExternalInboundMessageHeader::default();
        message_header.dst = message.dest.clone();

        let mut msg = Message::with_ext_in_header(message_header);
        if let Some(body) = message.body {
            msg.set_body(body);
        }

        if message.run_local {
            return self.run_local(abi, &message.dest, &msg).await;
        }

        unimplemented!()
    }
}

fn parse_account_stuff(raw: &[u8]) -> TransportResult<AccountStuff> {
    let mut slice = ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(raw))
        .and_then(|cell| {
            let mut slice: SliceData = cell.into();
            slice.move_by(1)?;
            Ok(slice)
        })
        .map_err(|e| TransportError::FailedToParseAccountState {
            reason: e.to_string(),
        })?;

    ton_block::AccountStuff::construct_from(&mut slice).map_err(|e| {
        TransportError::FailedToParseAccountState {
            reason: e.to_string(),
        }
    })
}

fn process_out_messages(
    messages: &Vec<Message>,
    abi_function: &AbiFunction,
) -> TransportResult<ContractOutput> {
    if messages.len() == 0 || !abi_function.has_output() {
        return Ok(ContractOutput {
            transaction_id: None,
            tokens: Vec::new(),
        });
    }

    for msg in messages {
        if !matches!(msg.header(), CommonMsgInfo::ExtOutMsgInfo(_)) {
            continue;
        }

        let body = msg.body().ok_or_else(|| TransportError::ExecutionError {
            reason: "output message has not body".to_string(),
        })?;

        if abi_function
            .is_my_output_message(body.clone(), false)
            .map_err(|e| TransportError::ExecutionError {
                reason: e.to_string(),
            })?
        {
            let tokens = abi_function.decode_output(body, false).map_err(|e| {
                TransportError::ExecutionError {
                    reason: e.to_string(),
                }
            })?;

            return Ok(ContractOutput {
                transaction_id: None,
                tokens,
            });
        }
    }

    return Err(TransportError::ExecutionError {
        reason: "no external output messages".to_owned(),
    });
}

fn to_api_error(e: TonlibError) -> TransportError {
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
            "-1:17519bc2a04b6ecf7afa25ba30601a4e16c9402979c236db13e1c6f3c4674e8c",
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

    #[tokio::test]
    async fn test_init() {
        let transport = make_transport().await;

        let account_state = transport.get_account_state(&elector_addr()).await.unwrap();
        println!("Account state: {:?}", account_state);
    }
}
