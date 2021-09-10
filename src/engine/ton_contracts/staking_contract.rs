use nekoton_abi::FunctionBuilder;
use once_cell::sync::OnceCell;
use ton_abi::{Function, ParamType};

use super::models::*;

/// External responsible function
pub fn is_active() -> &'static Function {
    static FUNCTION: OnceCell<Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("isActive")
            .time_header()
            .output("is_active", ParamType::Bool)
            .build()
    })
}

/// External function
pub fn current_relay_round() -> &'static Function {
    static FUNCTION: OnceCell<Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("currentRelayRound")
            .time_header()
            .output("round", ParamType::Uint(32))
            .build()
    })
}

/// External responsible function
pub fn get_relay_round_address() -> &'static Function {
    static FUNCTION: OnceCell<Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getRelayRoundAddress")
            .time_header()
            .input("round_num", ParamType::Uint(32))
            .output("address", ParamType::Address)
            .build()
    })
}

/// External responsible function
pub fn get_relay_round_address_from_timestamp() -> &'static Function {
    static FUNCTION: OnceCell<Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getRelayRoundAddressFromTimestamp")
            .time_header()
            .input("time", ParamType::Uint(32))
            .output("address", ParamType::Address)
            .build()
    })
}

/// External function
pub fn current_relay_round_start_time() -> &'static Function {
    static FUNCTION: OnceCell<Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("currentRelayRoundStartTime")
            .time_header()
            .output("start_time", ParamType::Uint(32))
            .build()
    })
}

/// External function
pub fn confirm_eth_account() -> &'static Function {
    static FUNCTION: OnceCell<Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("confirmEthAccount")
            .input("staker_addr", ParamType::Address)
            .input("eth_address", ParamType::Uint(160))
            .input("send_gas_to", ParamType::Address)
            .time_header()
            .build()
    })
}

pub mod events {
    use nekoton_abi::{EventBuilder, KnownParamTypePlain};
    use ton_abi::Event;

    use super::*;
    use ethabi::Param;

    pub fn relay_round_initialized() -> &'static Event {
        static EVENT: OnceCell<Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("RelayRoundInitialized")
                .inputs(RelayRoundInitializedEvent::param_type())
                .build()
        })
    }

    pub fn bridge_updated() -> &'static Event {
        static EVENT: OnceCell<Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("BridgeUpdated")
                .input("new_bridge", ParamType::Address)
                .build()
        })
    }

    pub fn election_ended() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("ElectionEnded")
                .input("round_num", ParamType::Uint(128))
                .build()
        })
    }

    pub fn election_started() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("ElectionStarted")
                .input("round_num", ParamType::Uint(128))
                .input("election_start_time", ParamType::Uint(128))
                .input("election_addr", ParamType::Address)
                .build()
        })
    }
}
