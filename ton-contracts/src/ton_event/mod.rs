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
    pub struct TonEventAbi;

    impl TonEventAbi {
        pub fn confirm() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("confirm");
                let input = vec![Param {
                    name: "signature".to_string(),
                    kind: ParamType::Bytes,
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn constructor() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("constructor");
                let input = vec![
                    Param {
                        name: "_initializer".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "_meta".to_string(),
                        kind: ParamType::Cell,
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn decode_configuration_meta() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("decodeConfigurationMeta");
                let input = vec![Param {
                    name: "data".to_string(),
                    kind: ParamType::Cell,
                }];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "rootToken".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn decode_ethereum_event_data() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("decodeEthereumEventData");
                let input = vec![Param {
                    name: "data".to_string(),
                    kind: ParamType::Cell,
                }];
                builder = builder.inputs(input);
                let output = vec![
                    Param {
                        name: "tokens".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "wid".to_string(),
                        kind: ParamType::Int(8),
                    },
                    Param {
                        name: "owner_addr".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "owner_pubkey".to_string(),
                        kind: ParamType::Uint(256),
                    },
                ];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn decode_ton_event_data() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("decodeTonEventData");
                let input = vec![Param {
                    name: "data".to_string(),
                    kind: ParamType::Cell,
                }];
                builder = builder.inputs(input);
                let output = vec![
                    Param {
                        name: "wid".to_string(),
                        kind: ParamType::Int(8),
                    },
                    Param {
                        name: "addr".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "tokens".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "ethereum_address".to_string(),
                        kind: ParamType::Uint(160),
                    },
                ];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn encode_configuration_meta() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("encodeConfigurationMeta");
                let input = vec![Param {
                    name: "rootToken".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "data".to_string(),
                    kind: ParamType::Cell,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn encode_ethereum_event_data() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("encodeEthereumEventData");
                let input = vec![
                    Param {
                        name: "tokens".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "wid".to_string(),
                        kind: ParamType::Int(8),
                    },
                    Param {
                        name: "owner_addr".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "owner_pubkey".to_string(),
                        kind: ParamType::Uint(256),
                    },
                ];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "data".to_string(),
                    kind: ParamType::Cell,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn encode_ton_event_data() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("encodeTonEventData");
                let input = vec![
                    Param {
                        name: "wid".to_string(),
                        kind: ParamType::Int(8),
                    },
                    Param {
                        name: "addr".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "tokens".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "ethereum_address".to_string(),
                        kind: ParamType::Uint(160),
                    },
                ];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "data".to_string(),
                    kind: ParamType::Cell,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn get_decoded_data() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("getDecodedData");
                let input = vec![Param {
                    name: "_answer_id".to_string(),
                    kind: ParamType::Uint(32),
                }];
                builder = builder.inputs(input);
                let output = vec![
                    Param {
                        name: "rootToken".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "wid".to_string(),
                        kind: ParamType::Int(8),
                    },
                    Param {
                        name: "addr".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "tokens".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "ethereum_address".to_string(),
                        kind: ParamType::Uint(160),
                    },
                    Param {
                        name: "owner_address".to_string(),
                        kind: ParamType::Address,
                    },
                ];
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
                        name: "_eventInitData".to_string(),
                        kind: ParamType::Tuple(vec![
                            Param {
                                name: "voteData".to_string(),
                                kind: ParamType::Tuple(vec![
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
                                ]),
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
                        ]),
                    },
                    Param {
                        name: "_status".to_string(),
                        kind: ParamType::Uint(8),
                    },
                    Param {
                        name: "confirms".to_string(),
                        kind: ParamType::Array(Box::new(ParamType::Uint(256))),
                    },
                    Param {
                        name: "rejects".to_string(),
                        kind: ParamType::Array(Box::new(ParamType::Uint(256))),
                    },
                    Param {
                        name: "empty".to_string(),
                        kind: ParamType::Array(Box::new(ParamType::Uint(256))),
                    },
                    Param {
                        name: "_signatures".to_string(),
                        kind: ParamType::Array(Box::new(ParamType::Bytes)),
                    },
                    Param {
                        name: "balance".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "_initializer".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "_meta".to_string(),
                        kind: ParamType::Cell,
                    },
                    Param {
                        name: "_requiredVotes".to_string(),
                        kind: ParamType::Uint(32),
                    },
                ];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn get_voters() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("getVoters");
                let input = vec![
                    Param {
                        name: "_answer_id".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "vote".to_string(),
                        kind: ParamType::Uint(8),
                    },
                ];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "voters".to_string(),
                    kind: ParamType::Array(Box::new(ParamType::Uint(256))),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn initializer() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("initializer");
                let output = vec![Param {
                    name: "initializer".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn meta() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("meta");
                let output = vec![Param {
                    name: "meta".to_string(),
                    kind: ParamType::Cell,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn receive_round_address() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("receiveRoundAddress");
                let input = vec![Param {
                    name: "roundContract".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn receive_round_relays() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("receiveRoundRelays");
                let input = vec![Param {
                    name: "keys".to_string(),
                    kind: ParamType::Array(Box::new(ParamType::Uint(256))),
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn required_votes() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("requiredVotes");
                let output = vec![Param {
                    name: "requiredVotes".to_string(),
                    kind: ParamType::Uint(32),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn signatures() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("signatures");
                let output = vec![Param {
                    name: "signatures".to_string(),
                    kind: ParamType::Map(
                        Box::new(ParamType::Uint(256)),
                        Box::new(ParamType::Bytes),
                    ),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn status() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("status");
                let output = vec![Param {
                    name: "status".to_string(),
                    kind: ParamType::Uint(8),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn votes() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("votes");
                let output = vec![Param {
                    name: "votes".to_string(),
                    kind: ParamType::Map(
                        Box::new(ParamType::Uint(256)),
                        Box::new(ParamType::Uint(8)),
                    ),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct TupleStruct0 {
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

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct TupleStruct1 {
        #[abi(name = "voteData")]
        pub vote_data: TupleStruct0,
        #[serde(with = "nekoton_utils::serde_address")]
        pub configuration: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub staking: ton_block::MsgAddressInt,
        #[abi(name = "chainId")]
        pub chain_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct ConfirmInput {
        pub signature: Vec<u8>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct ConstructorInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub initializer: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_cell")]
        pub meta: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct DecodeConfigurationMetaInput {
        #[serde(with = "nekoton_utils::serde_cell")]
        pub data: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct DecodeConfigurationMetaOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub root_token: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct DecodeEthereumEventDataInput {
        #[serde(with = "nekoton_utils::serde_cell")]
        pub data: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct DecodeEthereumEventDataOutput {
        pub tokens: num_bigint::BigUint,
        pub wid: i8,
        pub owner_addr: ton_types::UInt256,
        pub owner_pubkey: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct DecodeTonEventDataInput {
        #[serde(with = "nekoton_utils::serde_cell")]
        pub data: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct DecodeTonEventDataOutput {
        pub wid: i8,
        pub addr: ton_types::UInt256,
        pub tokens: num_bigint::BigUint,
        pub ethereum_address: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct EncodeConfigurationMetaInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub root_token: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct EncodeConfigurationMetaOutput {
        #[serde(with = "nekoton_utils::serde_cell")]
        pub data: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct EncodeEthereumEventDataInput {
        pub tokens: num_bigint::BigUint,
        pub wid: i8,
        pub owner_addr: ton_types::UInt256,
        pub owner_pubkey: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct EncodeEthereumEventDataOutput {
        #[serde(with = "nekoton_utils::serde_cell")]
        pub data: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct EncodeTonEventDataInput {
        pub wid: i8,
        pub addr: ton_types::UInt256,
        pub tokens: num_bigint::BigUint,
        pub ethereum_address: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct EncodeTonEventDataOutput {
        #[serde(with = "nekoton_utils::serde_cell")]
        pub data: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetDecodedDataInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct GetDecodedDataOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub root_token: ton_block::MsgAddressInt,
        pub wid: i8,
        pub addr: ton_types::UInt256,
        pub tokens: num_bigint::BigUint,
        pub ethereum_address: num_bigint::BigUint,
        #[serde(with = "nekoton_utils::serde_address")]
        pub owner_address: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetDetailsInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct GetDetailsOutput {
        #[abi(name = "_eventInitData")]
        pub event_init_data: TupleStruct1,
        #[abi(name = "_status")]
        pub status: u8,
        pub confirms: ton_types::UInt256,
        pub rejects: ton_types::UInt256,
        pub empty: ton_types::UInt256,
        #[abi(name = "_signatures")]
        pub signatures: Vec<u8>,
        pub balance: num_bigint::BigUint,
        #[serde(with = "nekoton_utils::serde_address")]
        pub initializer: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_cell")]
        pub meta: ton_types::Cell,
        #[abi(name = "_requiredVotes")]
        pub required_votes: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct GetVotersInput {
        #[abi(name = "_answer_id")]
        pub answer_id: u32,
        pub vote: u8,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct GetVotersOutput {
        pub voters: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct InitializerOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub initializer: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct MetaOutput {
        #[serde(with = "nekoton_utils::serde_cell")]
        pub meta: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct ReceiveRoundAddressInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub round_contract: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct ReceiveRoundRelaysInput {
        pub keys: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RequiredVotesOutput {
        #[abi(name = "requiredVotes")]
        pub required_votes: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct SignaturesOutput {
        pub signatures: HashMap<ton_types::UInt256, Vec<u8>>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct StatusOutput {
        pub status: u8,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct VotesOutput {
        pub votes: HashMap<ton_types::UInt256, u8>,
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
    pub struct TonEventAbi;

    impl TonEventAbi {
        pub fn confirm() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("Confirm");
                let input = vec![
                    Param {
                        name: "relay".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "signature".to_string(),
                        kind: ParamType::Bytes,
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
        pub signature: Vec<u8>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct RejectInput {
        pub relay: ton_types::UInt256,
    }
}
