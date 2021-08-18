use nekoton_abi::*;
use once_cell::sync::OnceCell;

/// External responsible function
pub fn is_active() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("isActive")
            .time_header()
            .out_arg("is_active", ton_abi::ParamType::Bool)
            .build()
    })
}

/// External function
pub fn current_relay_round() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("currentRelayRound")
            .time_header()
            .out_arg("round", ton_abi::ParamType::Uint(32))
            .build()
    })
}

/// External responsible function
pub fn get_relay_round_address() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getRelayRoundAddress")
            .time_header()
            .in_arg("round_num", ton_abi::ParamType::Uint(32))
            .out_arg("address", ton_abi::ParamType::Address)
            .build()
    })
}

/// External responsible function
pub fn get_relay_round_address_from_timestamp() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getRelayRoundAddressFromTimestamp")
            .time_header()
            .in_arg("time", ton_abi::ParamType::Uint(32))
            .out_arg("address", ton_abi::ParamType::Address)
            .build()
    })
}

/// External function
pub fn current_relay_round_start_time() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("currentRelayRoundStartTime")
            .time_header()
            .out_arg("currentRelayRoundStartTime", ton_abi::ParamType::Uint(32))
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn relay_round_initialized() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("RelayRoundInitialized")
                .in_arg("round_num", ton_abi::ParamType::Uint(32))
                .in_arg("round_start_time", ton_abi::ParamType::Uint(32))
                .in_arg("round_addr", ton_abi::ParamType::Address)
                .in_arg("relays_count", ton_abi::ParamType::Uint(32))
                .in_arg("duplicate", ton_abi::ParamType::Bool)
                .build()
        })
    }

    pub fn bridge_updated() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("BridgeUpdated")
                .in_arg("new_bridge", ton_abi::ParamType::Address)
                .build()
        })
    }
}
