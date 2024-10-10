use nekoton_abi::*;

use super::{models::*, TON_ABI_VERSION};

/// External function
pub fn status() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("status")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .output("status", EventStatus::param_type())
            .build()
    })
}

/// External function
pub fn round_number() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("round_number")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .output("round_number", u32::param_type())
            .build()
    })
}

/// External function
pub fn created_at() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("createdAt")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .output("createdAt", u32::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_voters() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getVoters")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .input("vote", EventVote::param_type())
            .outputs(RelayKeys::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_api_version() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getApiVersion")
            .abi_version(TON_ABI_VERSION)
            .default_headers()
            .output("version", u32::param_type())
            .build()
    })
}

/// Internal function
pub fn receive_round_relays() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("receiveRoundRelays")
            .abi_version(TON_ABI_VERSION)
            .inputs(RelayKeys::param_type())
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn rejected() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("Rejected")
                .abi_version(TON_ABI_VERSION)
                .input("reason", u32::param_type())
                .build()
        })
    }
}
