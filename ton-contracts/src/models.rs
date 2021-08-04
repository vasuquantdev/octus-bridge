use std::collections::HashMap;

use nekoton_abi::{
    BuildTokenValue, FunctionBuilder, PackAbi, TokenValueExt, UnpackAbi, UnpackAbiPlain,
    UnpackerError, UnpackerResult,
};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use ton_abi::{Param, ParamType};

#[derive(Serialize, Deserialize, Debug, Clone, PackAbi, UnpackAbi)]
pub struct BridgeConfiguration {
    #[serde(with = "nekoton_utils::serde_address")]
    pub staking: ton_block::MsgAddressInt,
    pub active: bool,
    #[serde(with = "nekoton_utils::serde_cell")]
    pub connector_code: ton_types::Cell,
    #[abi(name = "connectorDeployValue")]
    pub connector_deploy_value: num_bigint::BigUint,
}
