pub mod functions {
    use nekoton_abi::{
        BuildTokenValue, FunctionBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackToken,
        UnpackerError, UnpackerResult,
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use ton_abi::{Param, ParamType};

    #[derive(Copy, Clone, Debug)]
    pub struct TonEventConfigurationAbi;

    impl TonEventConfigurationAbi {
        pub fn basic_configuration() -> FunctionBuilder {
            {
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
                builder
            }
        }

        pub fn constructor() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("constructor");
                let input = vec![Param {
                    name: "_owner".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.inputs(input);
                builder
            }
        }

        pub fn deploy_event() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("deployEvent");
                let input = vec![
                    Param {
                        name: "eventTransaction".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "eventTransactionLt".to_string(),
                        kind: ParamType::Uint(64),
                    },
                    Param {
                        name: "eventTimestamp".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventIndex".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventData".to_string(),
                        kind: ParamType::Cell,
                    },
                ];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "eventEmitter".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn derive_event_address() -> FunctionBuilder {
            {
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
                        name: "eventTransactionLt".to_string(),
                        kind: ParamType::Uint(64),
                    },
                    Param {
                        name: "eventTimestamp".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventIndex".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventData".to_string(),
                        kind: ParamType::Cell,
                    },
                ];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "eventContract".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn get_details() -> FunctionBuilder {
            {
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
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "proxy".to_string(),
                        kind: ParamType::Uint(160),
                    },
                    Param {
                        name: "startTimestamp".to_string(),
                        kind: ParamType::Uint(32),
                    },
                ];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn get_type() -> FunctionBuilder {
            {
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
                builder
            }
        }

        pub fn network_configuration() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("networkConfiguration");
                let output = vec![
                    Param {
                        name: "eventEmitter".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "proxy".to_string(),
                        kind: ParamType::Uint(160),
                    },
                    Param {
                        name: "startTimestamp".to_string(),
                        kind: ParamType::Uint(32),
                    },
                ];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn owner() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("owner");
                let output = vec![Param {
                    name: "owner".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn transfer_ownership() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("transferOwnership");
                let input = vec![Param {
                    name: "newOwner".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.inputs(input);
                builder
            }
        }

        pub fn update() -> FunctionBuilder {
            {
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
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "proxy".to_string(),
                        kind: ParamType::Uint(160),
                    },
                    Param {
                        name: "startTimestamp".to_string(),
                        kind: ParamType::Uint(32),
                    },
                ];
                builder = builder.inputs(input);
                builder
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
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

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct TupleStruct1 {
        #[abi(name = "eventTransaction")]
        pub event_transaction: ton_types::UInt256,
        #[abi(name = "eventTransactionLt")]
        pub event_transaction_lt: u64,
        #[abi(name = "eventTimestamp")]
        pub event_timestamp: u32,
        #[abi(name = "eventIndex")]
        pub event_index: u32,
        #[serde(with = "nekoton_utils::serde_cell")]
        pub event_data: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct TupleStruct4 {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_emitter: ton_block::MsgAddressInt,
        pub proxy: num_bigint::BigUint,
        #[abi(name = "startTimestamp")]
        pub start_timestamp: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct BasicConfigurationOutput {
        #[abi(name = "basicConfiguration")]
        pub basic_configuration: TupleStruct0,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct ConstructorInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub owner: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct DeployEventInput {
        #[abi(name = "eventVoteData")]
        pub event_vote_data: TupleStruct1,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct DeployEventOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_emitter: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct DeriveEventAddressInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
        #[abi(name = "eventVoteData")]
        pub event_vote_data: TupleStruct1,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct DeriveEventAddressOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_contract: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct GetDetailsInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct GetDetailsOutput {
        #[abi(name = "_basicConfiguration")]
        pub basic_configuration: TupleStruct0,
        #[abi(name = "_networkConfiguration")]
        pub network_configuration: TupleStruct4,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct GetTypeInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct GetTypeOutput {
        #[abi(name = "_type")]
        pub typ: u8,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct NetworkConfigurationOutput {
        #[abi(name = "networkConfiguration")]
        pub network_configuration: TupleStruct4,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct OwnerOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub owner: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct TransferOwnershipInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub new_owner: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct UpdateInput {
        #[abi(name = "_basicConfiguration")]
        pub basic_configuration: TupleStruct0,
        #[abi(name = "_networkConfiguration")]
        pub network_configuration: TupleStruct4,
    }
}

pub mod events {
    use nekoton_abi::{
        BuildTokenValue, EventBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackToken,
        UnpackerError, UnpackerResult,
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use ton_abi::{Param, ParamType};

    #[derive(Copy, Clone, Debug)]
    pub struct TonEventConfigurationAbi;

    impl TonEventConfigurationAbi {
        pub fn ownership_transferred() -> EventBuilder {
            {
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
                builder
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct OwnershipTransferredInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub previous_owner: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub new_owner: ton_block::MsgAddressInt,
    }
}
