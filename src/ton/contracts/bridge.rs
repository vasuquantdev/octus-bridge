pub mod functions {
    use nekoton_abi::{
        BuildTokenValue, FunctionBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackToken,
        UnpackerError, UnpackerResult,
    };
    use serde::{Deserialize, Serialize};
    use ton_abi::{Param, ParamType};

    #[derive(Copy, Clone, Debug)]
    pub struct BridgeAbi;

    impl BridgeAbi {
        pub fn _random_nonce() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("_randomNonce");
                let output = vec![Param {
                    name: "_randomNonce".to_string(),
                    kind: ParamType::Uint(256),
                }];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn bridge_configuration() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("bridgeConfiguration");
                let output = vec![
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
                ];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn connector_counter() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("connectorCounter");
                let output = vec![Param {
                    name: "connectorCounter".to_string(),
                    kind: ParamType::Uint(64),
                }];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn constructor() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("constructor");
                let input = vec![
                    Param {
                        name: "_owner".to_string(),
                        kind: ParamType::Address,
                    },
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
                ];
                builder = builder.inputs(input);
                builder
            }
        }

        pub fn deploy_connector() -> FunctionBuilder {
            {
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
                builder
            }
        }

        pub fn derive_connector_address() -> FunctionBuilder {
            {
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

        pub fn update_bridge_configuration() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("updateBridgeConfiguration");
                let input = vec![
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
                ];
                builder = builder.inputs(input);
                builder
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct TupleStruct0 {
        #[serde(with = "nekoton_utils::serde_address")]
        pub staking: ton_block::MsgAddressInt,
        pub active: bool,
        #[serde(with = "nekoton_utils::serde_cell")]
        pub connector_code: ton_types::Cell,
        #[abi(name = "connectorDeployValue")]
        pub connector_deploy_value: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct RandomNonceOutput {
        #[abi(name = "_randomNonce")]
        pub random_nonce: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct BridgeConfigurationOutput {
        #[abi(name = "bridgeConfiguration")]
        pub bridge_configuration: TupleStruct0,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct ConnectorCounterOutput {
        #[abi(name = "connectorCounter")]
        pub connector_counter: u64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct ConstructorInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub owner: ton_block::MsgAddressInt,
        #[abi(name = "_bridgeConfiguration")]
        pub bridge_configuration: TupleStruct0,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct DeployConnectorInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct DeployConnectorOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub connector: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct DeriveConnectorAddressInput {
        pub id: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct DeriveConnectorAddressOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub connector: ton_block::MsgAddressInt,
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
    pub struct UpdateBridgeConfigurationInput {
        #[abi(name = "_bridgeConfiguration")]
        pub bridge_configuration: TupleStruct0,
    }
}

pub mod events {
    use nekoton_abi::{
        BuildTokenValue, EventBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackToken,
        UnpackerError, UnpackerResult,
    };
    use serde::{Deserialize, Serialize};
    use ton_abi::{Param, ParamType};

    #[derive(Copy, Clone, Debug)]
    pub struct BridgeAbi;

    impl BridgeAbi {
        pub fn bridge_configuration_update() -> EventBuilder {
            {
                let mut builder = EventBuilder::new("BridgeConfigurationUpdate");
                let input = vec![
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
                ];
                builder = builder.inputs(input);
                builder
            }
        }

        pub fn connector_deployed() -> EventBuilder {
            {
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
                builder
            }
        }

        pub fn event_configuration_disabled() -> EventBuilder {
            {
                let mut builder = EventBuilder::new("EventConfigurationDisabled");
                let input = vec![Param {
                    name: "id".to_string(),
                    kind: ParamType::Uint(32),
                }];
                builder = builder.inputs(input);
                builder
            }
        }

        pub fn event_configuration_enabled() -> EventBuilder {
            {
                let mut builder = EventBuilder::new("EventConfigurationEnabled");
                let input = vec![
                    Param {
                        name: "id".to_string(),
                        kind: ParamType::Uint(32),
                    },
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
                ];
                builder = builder.inputs(input);
                builder
            }
        }

        pub fn event_configuration_updated() -> EventBuilder {
            {
                let mut builder = EventBuilder::new("EventConfigurationUpdated");
                let input = vec![
                    Param {
                        name: "id".to_string(),
                        kind: ParamType::Uint(32),
                    },
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
                ];
                builder = builder.inputs(input);
                builder
            }
        }

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
    pub struct TupleStruct0 {
        #[serde(with = "nekoton_utils::serde_address")]
        pub staking: ton_block::MsgAddressInt,
        pub active: bool,
        #[serde(with = "nekoton_utils::serde_cell")]
        pub connector_code: ton_types::Cell,
        #[abi(name = "connectorDeployValue")]
        pub connector_deploy_value: num_bigint::BigUint,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct TupleStruct1 {
        #[serde(with = "nekoton_utils::serde_address")]
        pub addr: ton_block::MsgAddressInt,
        pub status: bool,
        #[abi(name = "_type")]
        pub typ: u8,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct BridgeConfigurationUpdateInput {
        #[abi(name = "bridgeConfiguration")]
        pub bridge_configuration: TupleStruct0,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct ConnectorDeployedInput {
        pub id: u64,
        #[serde(with = "nekoton_utils::serde_address")]
        pub connector: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct EventConfigurationDisabledInput {
        pub id: u32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct EventConfigurationEnabledInput {
        pub id: u32,
        #[abi(name = "eventConfiguration")]
        pub event_configuration: TupleStruct1,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct EventConfigurationUpdatedInput {
        pub id: u32,
        #[abi(name = "eventConfiguration")]
        pub event_configuration: TupleStruct1,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct OwnershipTransferredInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub previous_owner: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub new_owner: ton_block::MsgAddressInt,
    }
}
