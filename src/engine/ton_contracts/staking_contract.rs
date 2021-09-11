use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

/// External responsible function
pub fn is_active() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("isActive")
            .pubkey_header()
            .time_header()
            .output("is_active", bool::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_details() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getDetails")
            .pubkey_header()
            .time_header()
            .output("details", StakingDetails::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_relay_rounds_details() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getRelayRoundsDetails")
            .pubkey_header()
            .time_header()
            .output("details", RelayRoundsDetails::param_type())
            .build()
    })
}

/// External responsible function
pub fn get_election_address() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getElectionAddress")
            .pubkey_header()
            .time_header()
            .input("round_num", u32::param_type())
            .output("address", ton_abi::ParamType::Address)
            .build()
    })
}

/// External responsible function
pub fn get_relay_round_address() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getRelayRoundAddress")
            .pubkey_header()
            .time_header()
            .input("round_num", u32::param_type())
            .output("address", ton_abi::ParamType::Address)
            .build()
    })
}

/// External responsible function
pub fn get_user_data_address() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getUserDataAddress")
            .pubkey_header()
            .time_header()
            .input("user", ton_abi::ParamType::Address)
            .output("address", ton_abi::ParamType::Address)
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn election_started() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("ElectionStarted")
                .inputs(ElectionStartedEvent::param_type())
                .build()
        })
    }

    pub fn election_ended() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("ElectionEnded")
                .inputs(ElectionEndedEvent::param_type())
                .build()
        })
    }

    pub fn relay_round_initialized() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("RelayRoundInitialized")
                .inputs(RelayRoundInitializedEvent::param_type())
                .build()
        })
    }
}
