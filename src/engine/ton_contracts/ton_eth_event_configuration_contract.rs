use nekoton_abi::*;

use super::{models::*, TON_ABI_VERSION};

/// External function
pub fn get_details() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getDetails")
            .abi_version(TON_ABI_VERSION)
            .time_header()
            .expire_header()
            .outputs(TonEthEventConfigurationDetails::param_type())
            .build()
    })
}

/// Internal function
pub fn set_end_timestamp() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("setEndTimestamp")
            .abi_version(TON_ABI_VERSION)
            .input("end_timestamp", ton_abi::ParamType::Uint(32))
            .build()
    })
}
