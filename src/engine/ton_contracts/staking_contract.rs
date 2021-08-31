use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

/// External responsible function
pub fn is_active() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("isActive")
            .time_header()
            .output("is_active", ton_abi::ParamType::Bool)
            .build()
    })
}

/// External function
pub fn current_relay_round() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("currentRelayRound")
            .time_header()
            .output("round", ton_abi::ParamType::Uint(32))
            .build()
    })
}

/// External responsible function
pub fn get_relay_round_address() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getRelayRoundAddress")
            .time_header()
            .input("round_num", ton_abi::ParamType::Uint(32))
            .output("address", ton_abi::ParamType::Address)
            .build()
    })
}

/// External responsible function
pub fn get_relay_round_address_from_timestamp() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getRelayRoundAddressFromTimestamp")
            .time_header()
            .input("time", ton_abi::ParamType::Uint(32))
            .output("address", ton_abi::ParamType::Address)
            .build()
    })
}

/// External function
pub fn current_relay_round_start_time() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("currentRelayRoundStartTime")
            .time_header()
            .output("start_time", ton_abi::ParamType::Uint(32))
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn relay_round_initialized() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("RelayRoundInitialized")
                .inputs(RelayRoundInitializedEvent::param_type())
                .build()
        })
    }

    pub fn bridge_updated() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("BridgeUpdated")
                .input("new_bridge", ton_abi::ParamType::Address)
                .build()
        })
    }
}
