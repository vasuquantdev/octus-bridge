use opg::*;

use relay_models::models::{EthTonConfirmationDataView, EthTonTransactionView, TxStatView};

use super::*;

pub fn get_api() -> String {
    let api = describe_api! {
        info: {
            title: "My super API",
            version: "0.0.0",
        },
        paths: {
            ("unlock"): {
                POST: {
                    summary: "Provide password to unlock relay",
                    body: Password,
                    200: String
                }
            },
            ("event_configurations"): {
                GET: {
                    200: String, // todo: change to models
                }
            },
            ("event_configurations" / "vote"): {
                POST: {
                    body: Voting,
                    200: (),
                }
            },
            ("status"): {
                GET: {
                    200: Status
                }
            },
            ("init"): {
                POST: {
                    summary: "Provide data to init relay",
                    body: InitData,
                    200: String,
                    400: String,
                    405: String
                }
            },
            ("status" / "pending"): {
                GET: {
                    200: Vec<EthTonTransactionView>
                }
            },
            ("status" / "failed"): {
                GET: {
                    200: Vec<EthTonTransactionView>
                }
            },
            ("status" / "eth") :{
                GET: {
                    200: HashMap<u64, EthTonConfirmationDataView>
                }
            },
            ("status" / "relay") :{
                GET: {
                    summary: "Returns object, where key is relay key and values is list of confirmed transactions",
                    200: HashMap<String, Vec<TxStatView>>
                }
            },
            ("status" / "failed" / "retry") :{
                GET: {
                    200: Vec<EthTonTransactionView>
                }
            },
        }
    };
    serde_yaml::to_string(&api).unwrap()
}
