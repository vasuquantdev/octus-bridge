use nekoton_abi::*;

/// External responsible function
pub fn wallet_of() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("walletOf")
            .abi_version(ton_abi::contract::ABI_VERSION_2_3)
            .default_headers()
            .input("walletOwner", ton_abi::ParamType::Address)
            .output("value0", ton_abi::ParamType::Address)
            .build()
    })
}
