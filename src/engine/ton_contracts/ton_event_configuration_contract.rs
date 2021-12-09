use nekoton_abi::*;

use super::models::*;

/// External function
pub fn get_details() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getDetails")
            .time_header()
            .expire_header()
            .outputs(TonEventConfigurationDetails::param_type())
            .build()
    })
}

/// Internal function
pub fn set_end_timestamp() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("setEndTimestamp")
            .input("end_timestamp", ton_abi::ParamType::Uint(32))
            .build()
    })
}

/// Internal function
pub fn deploy_event() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("deployEvent")
            .input("vote_data", TonEventVoteData::param_type())
            .build()
    })
}
