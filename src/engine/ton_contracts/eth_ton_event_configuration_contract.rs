use nekoton_abi::*;

use super::{models::*, TON_ABI_VERSION};

/// External function
pub fn get_details() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getDetails")
            .abi_version(TON_ABI_VERSION)
            .time_header()
            .expire_header()
            .outputs(EthTonEventConfigurationDetails::param_type())
            .build()
    })
}

/// Internal function
pub fn set_end_block_number() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("setEndBlockNumber")
            .abi_version(TON_ABI_VERSION)
            .input("end_block_number", ton_abi::ParamType::Uint(32))
            .build()
    })
}
