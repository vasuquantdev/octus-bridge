use nekoton_abi::*;

/// External responsible function
pub fn has_unclaimed_reward() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("hasUnclaimedReward")
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
            .time_header()
            .output("end_time", u32::param_type())
            .build()
    })
}
