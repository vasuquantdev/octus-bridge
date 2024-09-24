use crate::utils::ExistingContract;
use everscale_rpc_client::{ClientOptions, RpcClient};
use once_cell::sync::Lazy;
use std::str::FromStr;
use tokio::sync::OnceCell;
use ton_block::MsgAddressInt;
use url::Url;

use super::*;

async fn get_rpc_client() -> &'static RpcClient {
    static RPC_CLIENT: Lazy<OnceCell<RpcClient>> = Lazy::new(OnceCell::new);
    RPC_CLIENT
        .get_or_init(|| async {
            RpcClient::new(
                vec![Url::from_str("https://jrpc-ton.broxus.com/proto").unwrap()],
                ClientOptions::default(),
            )
            .await
            .unwrap()
        })
        .await
}

async fn get_existing_contract(address: &str) -> ExistingContract {
    let rpc_client = get_rpc_client().await;

    let state = rpc_client
        .get_contract_state(&MsgAddressInt::from_str(address).unwrap(), None)
        .await
        .unwrap()
        .unwrap();

    ExistingContract {
        account: state.account,
        last_transaction_id: state.last_transaction_id,
    }
}

#[cfg(feature = "ton")]
#[tokio::test]
async fn get_ton_eth_decoded_data_test() {
    let contract =
        get_existing_contract("0:5616ddb058f9ab1e3ceceed45c40c15f4f8ef6d99f43a6312ff623443c5468f0") // TON -> EVM native event
            .await;
    let data = TonEthEventContract(&contract).event_decoded_data().unwrap();

    assert_eq!(
        data.token.to_string(),
        "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe"
    );
    assert_eq!(data.name, "Tether USD");
    assert_eq!(data.symbol, "USD₮");
    assert_eq!(data.decimals, 6);
}

#[cfg(feature = "ton")]
#[tokio::test]
async fn get_eth_ton_decoded_data_test() {
    let contract =
        get_existing_contract("0:c9a7fdec418f5f020b20b600d0eb10df9a2eb9762b205a431c79787a26589d57") // EVM -> TON native event
            .await;
    let data = EthTonEventContract(&contract).event_decoded_data().unwrap();

    assert_eq!(
        data.token.to_string(),
        "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe" // USDT minter
    );
    assert_eq!(
        data.proxy.to_string(),
        "0:31f98edaf5cd92e674799c0e4bc5cd8e050e4e9401e29f1766f4d27ccc87377d" // Bridge Proxy
    );
    assert_eq!(
        data.token_wallet.to_string(),
        "0:b40d62f8f20e725cf64101c0a933693b2453d94405ee867fdf18f4d6956a29d1" // Bridge Proxy USDT Wallet
    );
}

#[cfg(feature = "ton")]
#[tokio::test]
async fn get_jetton_wallet_address_test() {
    let contract =
        get_existing_contract("0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe") // USDT minter
            .await;
    let owner_address = MsgAddressInt::from_str(
        "0:31f98edaf5cd92e674799c0e4bc5cd8e050e4e9401e29f1766f4d27ccc87377d", // Bridge Proxy
    )
    .unwrap();
    let wallet_address = JettonMinterContract(&contract)
        .get_wallet_address(&owner_address)
        .unwrap();

    assert_eq!(
        wallet_address.to_string(),
        "0:b40d62f8f20e725cf64101c0a933693b2453d94405ee867fdf18f4d6956a29d1" // Bridge Proxy USDT Wallet
    );
}
