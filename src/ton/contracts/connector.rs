pub mod functions {
    use nekoton_abi::{
        BuildTokenValue, FunctionBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackToken,
        UnpackerError, UnpackerResult,
    };
    use serde::{Deserialize, Serialize};
    use ton_abi::{Param, ParamType};

    #[derive(Copy, Clone, Debug)]
    pub struct ConnectorAbi;

    impl ConnectorAbi {
        pub fn constructor() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("constructor");
                let input = vec![
                    Param {
                        name: "_eventConfiguration".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "_owner".to_string(),
                        kind: ParamType::Address,
                    },
                ];
                builder = builder.inputs(input);
                builder
            }
        }

        pub fn enabled() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("enabled");
                let output = vec![Param {
                    name: "enabled".to_string(),
                    kind: ParamType::Bool,
                }];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn event_configuration() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("eventConfiguration");
                let output = vec![Param {
                    name: "eventConfiguration".to_string(),
                    kind: ParamType::Address,
                }];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn get_details() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("getDetails");
                let output = vec![
                    Param {
                        name: "_id".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "_eventConfiguration".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "_enabled".to_string(),
                        kind: ParamType::Bool,
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
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct ConstructorInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub owner: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct EnabledOutput {
        pub enabled: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct EventConfigurationOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct GetDetailsOutput {
        #[abi(name = "_id")]
        pub id: num_bigint::BigUint,
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
        #[abi(name = "_enabled")]
        pub enabled: bool,
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
}

pub mod events {
    use nekoton_abi::{
        BuildTokenValue, EventBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackToken,
        UnpackerError, UnpackerResult,
    };
    use serde::{Deserialize, Serialize};
    use ton_abi::{Param, ParamType};

    #[derive(Copy, Clone, Debug)]
    pub struct ConnectorAbi;

    impl ConnectorAbi {
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
