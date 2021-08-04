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
    pub struct EthereumEventConfigurationAbi;

    impl EthereumEventConfigurationAbi {
        pub fn basic_configuration() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("basicConfiguration");
                let output = vec![
                    Param {
                        name: "eventABI".to_string(),
                        kind: ParamType::Bytes,
                    },
                    Param {
                        name: "staking".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "eventInitialBalance".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "eventCode".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "meta".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "chainId".to_string(),
                        kind: ParamType::Uint(32),
                    },
                ];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn broxus_bridge_callback() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("broxusBridgeCallback");
                let input = vec![
                    Param {
                        name: "eventTransaction".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "eventIndex".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventData".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "eventBlockNumber".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventBlock".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "configuration".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "staking".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "chainId".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "gasBackAddress".to_string(),
                        kind: ParamType::Address,
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn constructor() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("constructor");
                let input = vec![Param {
                    name: "_owner".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn deploy_event() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("deployEvent");
                let input = vec![
                    Param {
                        name: "eventTransaction".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "eventIndex".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventData".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "eventBlockNumber".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventBlock".to_string(),
                        kind: ParamType::Uint(256),
                    },
                ];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "eventContract".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn derive_event_address() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("deriveEventAddress");
                let input = vec![
                    Param {
                        name: "_answer_id".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventTransaction".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "eventIndex".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventData".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "eventBlockNumber".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventBlock".to_string(),
                        kind: ParamType::Uint(256),
                    },
                ];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "eventContract".to_string(),
                    kind: ParamType::Address,
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
                let output = vec![
                    Param {
                        name: "eventABI".to_string(),
                        kind: ParamType::Bytes,
                    },
                    Param {
                        name: "staking".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "eventInitialBalance".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "eventCode".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "meta".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "chainId".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventEmitter".to_string(),
                        kind: ParamType::Uint(160),
                    },
                    Param {
                        name: "eventBlocksToConfirm".to_string(),
                        kind: ParamType::Uint(16),
                    },
                    Param {
                        name: "proxy".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "startBlockNumber".to_string(),
                        kind: ParamType::Uint(32),
                    },
                ];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn get_type() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("getType");
                let input = vec![Param {
                    name: "_answer_id".to_string(),
                    kind: ParamType::Uint(32),
                }];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "_type".to_string(),
                    kind: ParamType::Uint(8),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn network_configuration() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("networkConfiguration");
                let output = vec![
                    Param {
                        name: "eventEmitter".to_string(),
                        kind: ParamType::Uint(160),
                    },
                    Param {
                        name: "eventBlocksToConfirm".to_string(),
                        kind: ParamType::Uint(16),
                    },
                    Param {
                        name: "proxy".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "startBlockNumber".to_string(),
                        kind: ParamType::Uint(32),
                    },
                ];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn owner() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("owner");
                let output = vec![Param {
                    name: "owner".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn transfer_ownership() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("transferOwnership");
                let input = vec![Param {
                    name: "newOwner".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn update() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("update");
                let input = vec![
                    Param {
                        name: "eventABI".to_string(),
                        kind: ParamType::Bytes,
                    },
                    Param {
                        name: "staking".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "eventInitialBalance".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "eventCode".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "meta".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "chainId".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventEmitter".to_string(),
                        kind: ParamType::Uint(160),
                    },
                    Param {
                        name: "eventBlocksToConfirm".to_string(),
                        kind: ParamType::Uint(16),
                    },
                    Param {
                        name: "proxy".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "startBlockNumber".to_string(),
                        kind: ParamType::Uint(32),
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct TupleStruct0 {
        #[abi(name = "eventABI")]
        pub event_a_b_i: Vec<u8>,
        #[serde(with = "nekoton_utils::serde_address")]
        pub staking: ton_block::MsgAddressInt,
        #[abi(name = "eventInitialBalance")]
        pub event_initial_balance: num_bigint::BigUint,
        #[serde(with = "nekoton_utils::serde_cell")]
        pub event_code: ton_types::Cell,
        #[serde(with = "nekoton_utils::serde_cell")]
        pub meta: ton_types::Cell,
        #[abi(name = "chainId")]
        pub chain_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct TupleStruct1 {
        #[abi(name = "eventTransaction")]
        pub event_transaction: ton_types::UInt256,
        #[abi(name = "eventIndex")]
        pub event_index: u32,
        #[serde(with = "nekoton_utils::serde_cell")]
        pub event_data: ton_types::Cell,
        #[abi(name = "eventBlockNumber")]
        pub event_block_number: u32,
        #[abi(name = "eventBlock")]
        pub event_block: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct TupleStruct2 {
        #[abi(name = "voteData")]
        pub vote_data: TupleStruct1,
        #[serde(with = "nekoton_utils::serde_address")]
        pub configuration: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub staking: ton_block::MsgAddressInt,
        #[abi(name = "chainId")]
        pub chain_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct TupleStruct6 {
        #[abi(name = "eventEmitter")]
        pub event_emitter: num_bigint::BigUint,
        #[abi(name = "eventBlocksToConfirm")]
        pub event_blocks_to_confirm: u16,
        #[serde(with = "nekoton_utils::serde_address")]
        pub proxy: ton_block::MsgAddressInt,
        #[abi(name = "startBlockNumber")]
        pub start_block_number: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct BasicConfigurationOutput {
        #[abi(name = "basicConfiguration")]
        pub basic_configuration: TupleStruct0,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct BroxusBridgeCallbackInput {
        #[abi(name = "eventInitData")]
        pub event_init_data: TupleStruct2,
        #[serde(with = "nekoton_utils::serde_address")]
        pub gas_back_address: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct ConstructorInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub owner: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct DeployEventInput {
        #[abi(name = "eventVoteData")]
        pub event_vote_data: TupleStruct1,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct DeployEventOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_contract: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct DeriveEventAddressInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
        #[abi(name = "eventVoteData")]
        pub event_vote_data: TupleStruct1,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct DeriveEventAddressOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_contract: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetDetailsInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct GetDetailsOutput {
        #[abi(name = "_basicConfiguration")]
        pub basic_configuration: TupleStruct0,
        #[abi(name = "_networkConfiguration")]
        pub network_configuration: TupleStruct6,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetTypeInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct GetTypeOutput {
        #[abi(name = "_type")]
        pub ty: u8,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct NetworkConfigurationOutput {
        #[abi(name = "networkConfiguration")]
        pub network_configuration: TupleStruct6,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct OwnerOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub owner: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct TransferOwnershipInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub new_owner: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct UpdateInput {
        #[abi(name = "_basicConfiguration")]
        pub basic_configuration: TupleStruct0,
        #[abi(name = "_networkConfiguration")]
        pub network_configuration: TupleStruct6,
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
    pub struct EthereumEventConfigurationAbi;

    impl EthereumEventConfigurationAbi {
        pub fn confirm() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("Confirm");
                let input = vec![Param {
                    name: "relay".to_string(),
                    kind: ParamType::Uint(256),
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn ownership_transferred() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("OwnershipTransferred");
                let input = vec![
                    Param {
                        name: "previousOwner".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "newOwner".to_string(),
                        kind: ParamType::Address,
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn reject() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("Reject");
                let input = vec![Param {
                    name: "relay".to_string(),
                    kind: ParamType::Uint(256),
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct ConfirmInput {
        pub relay: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct OwnershipTransferredInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub previous_owner: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub new_owner: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct RejectInput {
        pub relay: ton_types::UInt256,
    }
}
