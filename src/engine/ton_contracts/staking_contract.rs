use nekoton_abi::*;

use super::models::*;

pub fn start_election_on_new_round() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("startElectionOnNewRound")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .build()
    })
}

pub fn end_election() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("endElection")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .build()
    })
}

/// External responsible function
pub fn get_details() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getDetails")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .output("details", StakingDetails::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_relay_rounds_details() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getRelayRoundsDetails")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .output("details", RelayRoundsDetails::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_relay_config() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getRelayConfig")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .output("details", RelayConfigDetails::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_election_address() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getElectionAddress")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .input("round_num", u32::param_type())
            .output("address", ton_abi::ParamType::Address)
            .build()
    })
}

/// External responsible function
pub fn get_relay_round_address() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getRelayRoundAddress")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .input("round_num", u32::param_type())
            .output("address", ton_abi::ParamType::Address)
            .build()
    })
}

/// External responsible function
pub fn get_user_data_address() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getUserDataAddress")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .default_headers()
            .input("user", ton_abi::ParamType::Address)
            .output("address", ton_abi::ParamType::Address)
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn election_started() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("ElectionStarted")
                .abi_version(ton_abi::contract::ABI_VERSION_2_2)
                .inputs(ElectionStartedEvent::param_type())
                .build()
        })
    }

    pub fn election_ended() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("ElectionEnded")
                .abi_version(ton_abi::contract::ABI_VERSION_2_2)
                .inputs(ElectionEndedEvent::param_type())
                .build()
        })
    }

    pub fn relay_round_initialized() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("RelayRoundInitialized")
                .abi_version(ton_abi::contract::ABI_VERSION_2_2)
                .inputs(RelayRoundInitializedEvent::param_type())
                .build()
        })
    }

    pub fn relay_config_updated() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("RelayConfigUpdated")
                .abi_version(ton_abi::contract::ABI_VERSION_2_2)
                .inputs(RelayConfigUpdatedEvent::param_type())
                .build()
        })
    }
}
