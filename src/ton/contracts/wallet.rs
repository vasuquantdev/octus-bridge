pub mod functions {
    use nekoton_abi::{
        BuildTokenValue, FunctionBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackToken,
        UnpackerError, UnpackerResult,
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use ton_abi::{Param, ParamType};

    #[derive(Copy, Clone, Debug)]
    pub struct WalletAbi;

    impl WalletAbi {
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

        pub fn owner() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("owner");
                let output = vec![Param {
                    name: "owner".to_string(),
                    kind: ParamType::Uint(256),
                }];
                builder = builder.outputs(output);
                builder
            }
        }

        pub fn send_transaction() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("sendTransaction");
                let input = vec![
                    Param {
                        name: "dest".to_string(),
                        kind: ParamType::Address,
                    },
                    Param {
                        name: "value".to_string(),
                        kind: ParamType::Uint(128),
                    },
                    Param {
                        name: "bounce".to_string(),
                        kind: ParamType::Bool,
                    },
                    Param {
                        name: "flags".to_string(),
                        kind: ParamType::Uint(8),
                    },
                    Param {
                        name: "payload".to_string(),
                        kind: ParamType::Cell,
                    },
                ];
                builder = builder.inputs(input);
                builder
            }
        }

        pub fn transfer_ownership() -> FunctionBuilder {
            {
                let mut builder = FunctionBuilder::new("transferOwnership");
                let input = vec![Param {
                    name: "newOwner".to_string(),
                    kind: ParamType::Uint(256),
                }];
                builder = builder.inputs(input);
                builder
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct RandomNonceOutput {
        #[abi(name = "_randomNonce")]
        pub random_nonce: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct OwnerOutput {
        pub owner: ton_types::UInt256,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct SendTransactionInput {
        #[serde(with = "nekoton_utils::serde_address")]
        pub dest: ton_block::MsgAddressInt,
        pub value: num_bigint::BigUint,
        pub bounce: bool,
        pub flags: u8,
        #[serde(with = "nekoton_utils::serde_cell")]
        pub payload: ton_types::Cell,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct TransferOwnershipInput {
        #[abi(name = "newOwner")]
        pub new_owner: ton_types::UInt256,
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
    pub struct WalletAbi;

    impl WalletAbi {
        pub fn ownership_transferred() -> EventBuilder {
            {
                let mut builder = EventBuilder::new("OwnershipTransferred");
                let input = vec![
                    Param {
                        name: "previousOwner".to_string(),
                        kind: ParamType::Uint(256),
                    },
                    Param {
                        name: "newOwner".to_string(),
                        kind: ParamType::Uint(256),
                    },
                ];
                builder = builder.inputs(input);
                builder
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, UnpackAbi, PackAbi)]
    pub struct OwnershipTransferredInput {
        #[abi(name = "previousOwner")]
        pub previous_owner: ton_types::UInt256,
        #[abi(name = "newOwner")]
        pub new_owner: ton_types::UInt256,
    }
}
