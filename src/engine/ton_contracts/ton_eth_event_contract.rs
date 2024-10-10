use nekoton_abi::*;

use super::{models::*, TON_ABI_VERSION};

/// External responsible function
pub fn get_event_init_data() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getEventInitData")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .output("details", TonEthEventInitData::param_type())
            .build()
    })
}

/// External responsible function
#[cfg(feature = "ton")]
pub fn get_decoded_data() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getDecodedData")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .outputs(TonEthEventDecodedData::param_type())
            .build()
    })
}

/// External function
pub fn confirm() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("confirm")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .input("signature", ton_abi::ParamType::Bytes)
            .input("voteReceiver", ton_abi::ParamType::Address)
            .build()
    })
}

/// External function
pub fn reject() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("reject")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .input("voteReceiver", ton_abi::ParamType::Address)
            .build()
    })
}
