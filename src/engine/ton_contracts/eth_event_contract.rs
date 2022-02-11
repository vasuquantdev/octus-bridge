use nekoton_abi::*;

use super::models::*;

/// External responsible function
pub fn get_event_init_data() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getEventInitData")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .output("details", EthEventInitData::param_type())
            .build()
    })
}

/// External function
pub fn confirm() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("confirm")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .input("voteReceiver", ton_abi::ParamType::Address)
            .build()
    })
}

/// External function
pub fn reject() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("reject")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .input("voteReceiver", ton_abi::ParamType::Address)
            .build()
    })
}
