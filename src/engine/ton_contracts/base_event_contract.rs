use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

/// External function
pub fn status() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("status")
            .default_headers()
            .output("status", EventStatus::param_type())
            .build()
    })
}

/// External function
pub fn round_number() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("round_number")
            .default_headers()
            .output("round_number", u32::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_voters() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getVoters")
            .default_headers()
            .input("vote", EventVote::param_type())
            .outputs(RelayKeys::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_api_version() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getApiVersion")
            .default_headers()
            .output("version", u32::param_type())
            .build()
    })
}

/// Internal function
pub fn receive_round_relays() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("receiveRoundRelays")
            .inputs(RelayKeys::param_type())
            .build()
    })
}
