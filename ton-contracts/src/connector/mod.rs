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
    pub struct ConnectorAbi;

    impl ConnectorAbi {
        pub fn constructor() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
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
                builder.build()
            })
        }

        pub fn enabled() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("enabled");
                let output = vec![Param {
                    name: "enabled".to_string(),
                    kind: ParamType::Bool,
                }];
                builder = builder.outputs(output);
                builder.build()
            })
        }

        pub fn event_configuration() -> &'static ton_abi::Function {
            static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
            FUNCTION.get_or_init(|| {
                let mut builder = FunctionBuilder::new("eventConfiguration");
                let output = vec![Param {
                    name: "eventConfiguration".to_string(),
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
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
    pub struct ConstructorInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub owner: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct EnabledOutput {
        pub enabled: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct EventConfigurationOutput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbiPlain)]
    pub struct GetDetailsOutput {
        #[abi(name = "_id")]
        pub id: u128,
        #[serde(with = "nekoton_utils::serde_address")]
        pub event_configuration: ton_block::MsgAddressInt,
        #[abi(name = "_enabled")]
        pub enabled: bool,
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
    pub struct ConnectorAbi;

    impl ConnectorAbi {
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
    pub struct OwnershipTransferredInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub previous_owner: ton_block::MsgAddressInt,
        #[serde(with = "nekoton_utils::serde_address")]
        pub new_owner: ton_block::MsgAddressInt,
    }
}
