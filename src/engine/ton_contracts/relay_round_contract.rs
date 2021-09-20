use nekoton_abi::*;
use once_cell::sync::OnceCell;

/// External responsible function
pub fn has_unclaimed_reward() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("hasUnclaimedReward")
            .time_header()
            .input("staker_addr", ton_abi::ParamType::Address)
            .output("has_reward", ton_abi::ParamType::Bool)
            .build()
    })
}

/// External function
pub fn end_time() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("end_time")
            .time_header()
            .output("end_time", u32::param_type())
            .build()
    })
}
