use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

/// External responsible function
pub fn get_details() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getDetails")
            .default_headers()
            .outputs(EthEventDetails::param_type())
            .build()
    })
}

/// Internal function
pub fn receive_round_relays() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("receiveRoundRelays")
            .input(
                "keys",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .build()
    })
}

/// External function
pub fn confirm() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| FunctionBuilder::new("confirm").default_headers().build())
}

/// External function
pub fn reject() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| FunctionBuilder::new("reject").default_headers().build())
}
