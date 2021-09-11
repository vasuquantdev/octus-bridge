use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

/// External responsible function
pub fn get_event_init_data() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getEventInitData")
            .default_headers()
            .output("details", EthEventInitData::param_type())
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
