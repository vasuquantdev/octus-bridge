pub mod functions {
    use nekoton_abi::{
        BuildTokenValue, FunctionBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackAbiPlain,
        UnpackerError, UnpackerResult,
    };
    use once_cell::sync::OnceCell;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use ton_abi::{Param, ParamType};

    #[derive(Copy, Clone, Debug)]
    pub struct RelayRoundAbi;

    impl RelayRoundAbi {
        pub fn current_version() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("current_version");
                let output = vec![Param {
                    name: "current_version".to_string(),
                    kind: ParamType::Uint(32),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn get_details() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("getDetails");
                let input = vec![Param {
                    name: "_answer_id".to_string(),
                    kind: ParamType::Uint(32),
                }];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "value0".to_string(),
                    kind: ParamType::Tuple(vec![
                        Param {
                            name: "root".to_string(),
                            kind: ParamType::Address,
                        },
                        Param {
                            name: "round_num".to_string(),
                            kind: ParamType::Uint(128),
                        },
                        Param {
                            name: "relays".to_string(),
                            kind: ParamType::Array(Box::new(ParamType::Tuple(vec![
                                Param {
                                    name: "staker_addr".to_string(),
                                    kind: ParamType::Address,
                                },
                                Param {
                                    name: "ton_pubkey".to_string(),
                                    kind: ParamType::Uint(256),
                                },
                                Param {
                                    name: "eth_addr".to_string(),
                                    kind: ParamType::Uint(160),
                                },
                                Param {
                                    name: "staked_tokens".to_string(),
                                    kind: ParamType::Uint(128),
                                },
                                Param {
                                    name: "reward_claimed".to_string(),
                                    kind: ParamType::Bool,
                                },
                            ]))),
                        },
                        Param {
                            name: "relays_installed".to_string(),
                            kind: ParamType::Bool,
                        },
                        Param {
                            name: "code_version".to_string(),
                            kind: ParamType::Uint(32),
                        },
                    ]),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn get_relay_by_staker_address() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("getRelayByStakerAddress");
                let input = vec![
                    Param {
                        name: "_answer_id".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "staker_addr".to_string(),
                        kind: ParamType::Address,
                    },
                ];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "value0".to_string(),
                    kind: ParamType::Tuple(vec![
                        Param {
                            name: "staker_addr".to_string(),
                            kind: ParamType::Address,
                        },
                        Param {
                            name: "ton_pubkey".to_string(),
                            kind: ParamType::Uint(256),
                        },
                        Param {
                            name: "eth_addr".to_string(),
                            kind: ParamType::Uint(160),
                        },
                        Param {
                            name: "staked_tokens".to_string(),
                            kind: ParamType::Uint(128),
                        },
                        Param {
                            name: "reward_claimed".to_string(),
                            kind: ParamType::Bool,
                        },
                    ]),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn get_relays() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("getRelays");
                let input = vec![Param {
                    name: "send_gas_to".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn get_reward_for_round() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("getRewardForRound");
                let input = vec![
                    Param {
                        name: "staker_addr".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "send_gas_to".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "code_version".to_string(),
                        kind: ParamType::Uint(32),
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn get_user_data_address() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("getUserDataAddress");
                let input = vec![
                    Param {
                        name: "_answer_id".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "user".to_string(),
                        kind: ParamType::Address,
                    },
                ];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "value0".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn platform_code() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("platform_code");
                let output = vec![Param {
                    name: "platform_code".to_string(),
                    kind: ParamType::Cell,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn relay_keys() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("relayKeys");
                let input = vec![Param {
                    name: "_answer_id".to_string(),
                    kind: ParamType::Uint(32),
                }];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "value0".to_string(),
                    kind: ParamType::Array(Box::new(ParamType::Uint(256))),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn relays_count() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("relays_count");
                let output = vec![Param {
                    name: "relays_count".to_string(),
                    kind: ParamType::Uint(256),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn relays_installed() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("relays_installed");
                let output = vec![Param {
                    name: "relays_installed".to_string(),
                    kind: ParamType::Bool,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn reward_round_num() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("reward_round_num");
                let output = vec![Param {
                    name: "reward_round_num".to_string(),
                    kind: ParamType::Uint(128),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn root() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("root");
                let output = vec![Param {
                    name: "root".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn round_len() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("round_len");
                let output = vec![Param {
                    name: "round_len".to_string(),
                    kind: ParamType::Uint(128),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn round_num() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("round_num");
                let output = vec![Param {
                    name: "round_num".to_string(),
                    kind: ParamType::Uint(128),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn round_reward() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("round_reward");
                let output = vec![Param {
                    name: "round_reward".to_string(),
                    kind: ParamType::Uint(128),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn set_relays() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("setRelays");
                let input = vec![
                    Param {
                        name: "_relay_list".to_string(),
                        kind: ParamType::Array(Box::new(ParamType::Tuple(vec![
                            Param {
                                name: "staker_addr".to_string(),
                                kind: ParamType::Address,
                            },
                            Param {
                                name: "ton_pubkey".to_string(),
                                kind: ParamType::Uint(256),
                            },
                            Param {
                                name: "eth_addr".to_string(),
                                kind: ParamType::Uint(160),
                            },
                            Param {
                                name: "staked_tokens".to_string(),
                                kind: ParamType::Uint(128),
                            },
                            Param {
                                name: "reward_claimed".to_string(),
                                kind: ParamType::Bool,
                            },
                        ]))),
                    },
                    Param {
                        name: "send_gas_to".to_string(),
                        kind: ParamType::Address,
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn start_time() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("start_time");
                let output = vec![Param {
                    name: "start_time".to_string(),
                    kind: ParamType::Uint(128),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn total_tokens_staked() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("total_tokens_staked");
                let output = vec![Param {
                    name: "total_tokens_staked".to_string(),
                    kind: ParamType::Uint(128),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn upgrade() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("upgrade");
                let input = vec![
                    Param {
                        name: "code".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "new_version".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "send_gas_to".to_string(),
                        kind: ParamType::Address,
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct TupleStruct0 {
        #[serde(with = "nekoton_utils::serde_address")]
        pub staker_addr: ton_block::MsgAddressInt,
        pub ton_pubkey: ton_types::UInt256,
        pub eth_addr: num_bigint::BigUint,
        pub staked_tokens: num_bigint::BigUint,
        pub reward_claimed: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct TupleStruct1 {
        #[serde(with = "nekoton_utils::serde_address")]
        pub root: ton_block::MsgAddressInt,
        pub round_num: num_bigint::BigUint,
        pub relays: TupleStruct0,
        pub relays_installed: bool,
        pub code_version: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct CurrentVersionOutput {
        pub current_version: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetDetailsInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct GetDetailsOutput {
        pub value0: TupleStruct1,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetRelayByStakerAddressInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
        #[serde(with = "nekoton_utils::serde_address")]
        pub staker_addr: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct GetRelayByStakerAddressOutput {
        pub value0: TupleStruct0,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetRelaysInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub send_gas_to: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetRewardForRoundInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub staker_addr: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub send_gas_to: ton_block::MsgAddressInt,
        pub code_version: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetUserDataAddressInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
        #[serde(with = "nekoton_utils::serde_address")]
        pub user: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct GetUserDataAddressOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub value0: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct PlatformCodeOutput {
        #[serde(with = "nekoton_utils::serde_cell")]
        pub platform_code: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct RelayKeysInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RelayKeysOutput {
        pub value0: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RelaysCountOutput {
        pub relays_count: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RelaysInstalledOutput {
        pub relays_installed: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RewardRoundNumOutput {
        pub reward_round_num: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RootOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub root: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RoundLenOutput {
        pub round_len: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RoundNumOutput {
        pub round_num: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RoundRewardOutput {
        pub round_reward: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct SetRelaysInput {
        #[abi(name = "_relay_list")]
        pub relay_list: TupleStruct0,
        #[serde(with = "nekoton_utils::serde_address")]
        pub send_gas_to: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct StartTimeOutput {
        pub start_time: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct TotalTokensStakedOutput {
        pub total_tokens_staked: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct UpgradeInput {
        #[serde(with = "nekoton_utils::serde_cell")]
        pub code: ton_types::Cell,
        pub new_version: u32,
        #[serde(with = "nekoton_utils::serde_address")]
        pub send_gas_to: ton_block::MsgAddressInt,
    }
}

pub mod events {
    use nekoton_abi::{
        BuildTokenValue, EventBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackAbiPlain,
        UnpackerError, UnpackerResult,
    };
    use once_cell::sync::OnceCell;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use ton_abi::{Param, ParamType};

    #[derive(Copy, Clone, Debug)]
    pub struct RelayRoundAbi;

    impl RelayRoundAbi {
        pub fn relay_round_code_upgraded() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("RelayRoundCodeUpgraded");
                let input = vec![Param {
                    name: "code_version".to_string(),
                    kind: ParamType::Uint(32),
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct RelayRoundCodeUpgradedInput {
        pub code_version: u32,
    }
}
