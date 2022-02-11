use nekoton_abi::*;

use super::models::*;

/// External function
pub fn get_details() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getDetails")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
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
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .input("end_timestamp", ton_abi::ParamType::Uint(32))
            .build()
    })
}

/// Internal function
pub fn deploy_event() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("deployEvent")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .input("vote_data", TonEventVoteData::param_type())
            .build()
    })
}
