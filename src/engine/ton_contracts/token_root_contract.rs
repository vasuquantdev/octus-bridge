use nekoton_abi::*;

use super::TON_ABI_VERSION;

/// External responsible function
pub fn wallet_of() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("walletOf")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .input("walletOwner", ton_abi::ParamType::Address)
            .output("value0", ton_abi::ParamType::Address)
            .build()
    })
}
