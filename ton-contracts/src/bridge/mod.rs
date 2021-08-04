pub mod functions {
    use nekoton_abi::{
        BuildTokenValue, FunctionBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackAbiPlain,
        UnpackerError, UnpackerResult,
    };
    use once_cell::sync::OnceCell;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use ton_abi::{Param, ParamType};

    use crate::models::*;

    #[derive(Copy, Clone, Debug)]
    pub struct BridgeAbi;

    impl BridgeAbi {
        pub fn _random_nonce() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("_randomNonce");
                let output = vec![Param {
                    name: "_randomNonce".to_string(),
                    kind: ParamType::Uint(256),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn bridge_configuration() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("bridgeConfiguration");
                let output = vec![Param {
                    name: "bridgeConfiguration".to_string(),
                    kind: ParamType::Tuple(vec![
                        Param {
                            name: "staking".to_string(),
                            kind: ParamType::Address,
                        },
                        Param {
                            name: "active".to_string(),
                            kind: ParamType::Bool,
                        },
                        Param {
                            name: "connectorCode".to_string(),
                            kind: ParamType::Cell,
                        },
                        Param {
                            name: "connectorDeployValue".to_string(),
                            kind: ParamType::Uint(128),
                        },
                    ]),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn connector_counter() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("connectorCounter");
                let output = vec![Param {
                    name: "connectorCounter".to_string(),
                    kind: ParamType::Uint(64),
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn constructor() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("constructor");
                let input = vec![
                    Param {
                        name: "_owner".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "_bridgeConfiguration".to_string(),
                        kind: ParamType::Tuple(vec![
                            Param {
                                name: "staking".to_string(),
                                kind: ParamType::Address,
                            },
                            Param {
                                name: "active".to_string(),
                                kind: ParamType::Bool,
                            },
                            Param {
                                name: "connectorCode".to_string(),
                                kind: ParamType::Cell,
                            },
                            Param {
                                name: "connectorDeployValue".to_string(),
                                kind: ParamType::Uint(128),
                            },
                        ]),
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn deploy_connector() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("deployConnector");
                let input = vec![Param {
                    name: "_eventConfiguration".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "connector".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn derive_connector_address() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("deriveConnectorAddress");
                let input = vec![Param {
                    name: "id".to_string(),
                    kind: ParamType::Uint(128),
                }];
                builder = builder.inputs(input);
                let output = vec![Param {
                    name: "connector".to_string(),
                    kind: ParamType::Address,
                }];
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

        pub fn update_bridge_configuration() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("updateBridgeConfiguration");
                let input = vec![Param {
                    name: "_bridgeConfiguration".to_string(),
                    kind: ParamType::Tuple(vec![
                        Param {
                            name: "staking".to_string(),
                            kind: ParamType::Address,
                        },
                        Param {
                            name: "active".to_string(),
                            kind: ParamType::Bool,
                        },
                        Param {
                            name: "connectorCode".to_string(),
                            kind: ParamType::Cell,
                        },
                        Param {
                            name: "connectorDeployValue".to_string(),
                            kind: ParamType::Uint(128),
                        },
                    ]),
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct RandomNonceOutput {
        #[abi(name = "_randomNonce", with = "nekoton_abi::uint256_bytes")]
        #[serde(with = "nekoton_utils::serde_uint256")]
        pub random_nonce: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct BridgeConfigurationOutput {
        #[abi(name = "bridgeConfiguration")]
        pub bridge_configuration: BridgeConfiguration,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct ConnectorCounterOutput {
        #[abi(name = "connectorCounter")]
        pub connector_counter: u64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct ConstructorInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub owner: ton_block::MsgAddressInt,
        #[abi(name = "_bridgeConfiguration")]
        pub bridge_configuration: BridgeConfiguration,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct DeployConnectorInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct DeployConnectorOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub connector: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct DeriveConnectorAddressInput {
        pub id: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct DeriveConnectorAddressOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub connector: ton_block::MsgAddressInt,
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
    pub struct UpdateBridgeConfigurationInput {
        #[abi(name = "_bridgeConfiguration")]
        pub bridge_configuration: BridgeConfiguration,
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

    use crate::models::*;

    #[derive(Copy, Clone, Debug)]
    pub struct BridgeAbi;

    impl BridgeAbi {
        pub fn bridge_configuration_update() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("BridgeConfigurationUpdate");
                let input = vec![Param {
                    name: "bridgeConfiguration".to_string(),
                    kind: ParamType::Tuple(vec![
                        Param {
                            name: "staking".to_string(),
                            kind: ParamType::Address,
                        },
                        Param {
                            name: "active".to_string(),
                            kind: ParamType::Bool,
                        },
                        Param {
                            name: "connectorCode".to_string(),
                            kind: ParamType::Cell,
                        },
                        Param {
                            name: "connectorDeployValue".to_string(),
                            kind: ParamType::Uint(128),
                        },
                    ]),
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn connector_deployed() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("ConnectorDeployed");
                let input = vec![
                    Param {
                        name: "id".to_string(),
                        kind: ParamType::Uint(64),
                    },
                    Param {
                        name: "connector".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "eventConfiguration".to_string(),
                        kind: ParamType::Address,
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn event_configuration_disabled() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("EventConfigurationDisabled");
                let input = vec![Param {
                    name: "id".to_string(),
                    kind: ParamType::Uint(32),
                }];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn event_configuration_enabled() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("EventConfigurationEnabled");
                let input = vec![
                    Param {
                        name: "id".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventConfiguration".to_string(),
                        kind: ParamType::Tuple(vec![
                            Param {
                                name: "addr".to_string(),
                                kind: ParamType::Address,
                            },
                            Param {
                                name: "status".to_string(),
                                kind: ParamType::Bool,
                            },
                            Param {
                                name: "_type".to_string(),
                                kind: ParamType::Uint(8),
                            },
                        ]),
                    },
                ];
                builder = builder.inputs(input);
                builder.build()
            })
        }

        pub fn event_configuration_updated() -> &'static ton_abi::Event {
            static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
            EVENT.get_or_init(|| {
                let mut builder = EventBuilder::new("EventConfigurationUpdated");
                let input = vec![
                    Param {
                        name: "id".to_string(),
                        kind: ParamType::Uint(32),
                    },
                    Param {
                        name: "eventConfiguration".to_string(),
                        kind: ParamType::Tuple(vec![
                            Param {
                                name: "addr".to_string(),
                                kind: ParamType::Address,
                            },
                            Param {
                                name: "status".to_string(),
                                kind: ParamType::Bool,
                            },
                            Param {
                                name: "_type".to_string(),
                                kind: ParamType::Uint(8),
                            },
                        ]),
                    },
                ];
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
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct EventConfiguration {
        #[serde(with = "nekoton_utils::serde_address")]
        pub addr: ton_block::MsgAddressInt,
        pub status: bool,
        #[abi(name = "_type")]
        pub ty: u8,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct BridgeConfigurationUpdateInput {
        #[abi(name = "bridgeConfiguration")]
        pub bridge_configuration: BridgeConfiguration,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct ConnectorDeployedInput {
        pub id: u64,
        #[serde(with = "nekoton_utils::serde_address")]
        pub connector: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct EventConfigurationDisabledInput {
        pub id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct EventConfigurationEnabledInput {
        pub id: u32,
        #[abi(name = "eventConfiguration")]
        pub event_configuration: EventConfiguration,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct EventConfigurationUpdatedInput {
        pub id: u32,
        #[abi(name = "eventConfiguration")]
        pub event_configuration: EventConfiguration,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct OwnershipTransferredInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub previous_owner: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub new_owner: ton_block::MsgAddressInt,
    }
}
