use nekoton_abi::*;

use super::super::TON_ABI_VERSION;

/// Token root address
pub fn root() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("root")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .output("value0", ton_abi::ParamType::Address)
            .build()
    })
}
