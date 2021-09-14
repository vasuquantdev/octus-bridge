use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

/// External function
pub fn get_details() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getDetails")
            .time_header()
            .expire_header()
            .outputs(EthEventConfigurationDetails::param_type())
            .build()
    })
}

/// Internal function
pub fn set_end_block_number() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("setEndBlockNumber")
            .input("end_block_number", ton_abi::ParamType::Uint(32))
            .build()
    })
}

/// Internal function
pub fn deploy_event() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("deployEvent")
            .input("vote_data", EthEventVoteData::param_type())
            .build()
    })
}
