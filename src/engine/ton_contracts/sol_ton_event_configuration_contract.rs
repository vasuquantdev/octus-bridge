use nekoton_abi::*;

use super::models::*;

/// External function
pub fn get_details() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getDetails")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .time_header()
            .expire_header()
            .outputs(SolTonEventConfigurationDetails::param_type())
            .build()
    })
}

/// Internal function
pub fn set_end_block_number() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("setEndBlockNumber")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .input("end_block_number", ton_abi::ParamType::Uint(32))
            .build()
    })
}

/// Internal function
pub fn deploy_event() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("deployEvent")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .input("vote_data", SolTonEventVoteData::param_type())
            .build()
    })
}
