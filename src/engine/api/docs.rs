use opg::*;

use relay_models::models::*;

use super::*;

pub fn swagger() -> String {
    let api = describe_api! {
        info: {
            title: "Relay API",
            version: "0.0.1",
        },
        tags: {
            stuff("Methods for managing relay state"),
            event_configurations("Methods for managing event configurations"),
            eth_to_ton("Statistics for events emitted from ETH"),
            ton_to_eth("Statistics for events emitted from TON"),
        },
        paths: {
            ("init"): {
                POST: {
                    tags: { stuff },
                    summary: "Initialize relay with new keys",
                    body: InitData,
                    200: String,
                    400: String,
                    405: String
                }
            },
            ("unlock"): {
                POST: {
                    tags: { stuff },
                    summary: "Unlock relay with password",
                    body: Password,
                    200: String,
                }
            },
            ("retry-failed"): {
                POST: {
                    tags: { stuff },
                    summary: "Retry failed vote transactions",
                    200: Vec<EthTonTransactionView>,
                }
            },
            ("rescan-eth"): {
                POST: {
                    tags: { stuff },
                    summary: "Set current ETH block",
                    200: (),
                }
            },
            ("status"): {
                GET: {
                    tags: { stuff },
                    summary: "Brief relay info",
                    200: Status
                }
            },
            ("event-configurations"): {
                GET: {
                    tags: { event_configuration },
                    summary: "Get known event configurations",
                    200: Vec<EventConfigurationView>,
                },
                POST: {
                    tags: { event_configuration },
                    summary: "Create new event configuration",
                    body: NewEventConfiguration,
                    200: (),
                }
            },
            ("event-configurations" / "vote"): {
                POST: {
                    tags: { event_configurations },
                    summary: "Vote for new event configuration",
                    body: Voting,
                    200: (),
                }
            },
            ("event-configurations" / { configuration_id: u64 }): {
                GET: {
                    tags: { event_configuration },
                    summary: "Get event configuration by id",
                    200: EventConfigurationView,
                    400: (),
                }
            },
            ("eth-to-ton" / "pending"): {
                GET: {
                    tags: { eth_to_ton },
                    summary: "Pending votes for ETH-to-TON events",
                    200: Vec<EthTonTransactionView>
                }
            },
            ("eth-to-ton" / "failed"): {
                GET: {
                    tags: { eth_to_ton },
                    summary: "Failed votes for ETH-to-TON events",
                    200: Vec<EthTonTransactionView>
                }
            },
            ("eth-to-ton" / "queued"): {
                GET: {
                    tags: { eth_to_ton },
                    summary: "Verification queue",
                    200: HashMap<u64, EthEventVoteDataView>
                }
            },
            ("eth-to-ton" / "stats"): {
                GET: {
                    tags: { eth_to_ton },
                    summary: "Known votes for all relays",
                    200: HashMap<String, Vec<EthTxStatView>>
                }
            },
            ("ton-to-eth" / "pending"): {
                GET: {
                    tags: { ton_to_eth },
                    summary: "Pending votes for TON-to-ETH events",
                    200: Vec<TonEthTransactionView>,
                }
            },
            ("ton-to-eth" / "failed"): {
                GET: {
                    tags: { ton_to_eth },
                    summary: "Failed votes for TON-to-ETH events",
                    200: Vec<TonEthTransactionView>
                }
            },
            ("ton-to-eth" / "queued" / { configuration_id: u64 }): {
                GET: {
                    tags: { ton_to_eth },
                    summary: "Verification queue",
                    200: HashMap<u64, TonEventVoteDataView>,
                }
            },
            ("ton-to-eth" / "stats"): {
                GET: {
                    tags: { ton_to_eth },
                    summary: "Known votes for all relays",
                    200: HashMap<String, Vec<TonTxStatView>>
                }
            }
        }
    };
    serde_yaml::to_string(&api).unwrap()
}
