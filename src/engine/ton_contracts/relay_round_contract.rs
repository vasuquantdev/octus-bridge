use nekoton_abi::*;

use super::models::*;

/// External responsible function
pub fn get_details() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getDetails")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .time_header()
            .output("details", RelayRoundDetails::param_type())
            .build()
    })
}

/// External responsible function
pub fn has_unclaimed_reward() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("hasUnclaimedReward")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .time_header()
            .input("staker_addr", ton_abi::ParamType::Address)
            .output("has_reward", ton_abi::ParamType::Bool)
            .build()
    })
}

/// External function
pub fn end_time() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("end_time")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .time_header()
            .output("end_time", u32::param_type())
            .build()
    })
}
