use nekoton_abi::*;
use once_cell::sync::OnceCell;

pub fn current_relay_round() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("currentRelayRound")
            .time_header()
            .out_arg("currentRelayRound", ton_abi::ParamType::Uint(128))
            .build()
    })
}

pub fn get_relay_round_address() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("getRelayRoundAddress")
            .time_header()
            .in_arg("round_num", ton_abi::ParamType::Uint(128))
            .out_arg("value0", ton_abi::ParamType::Address)
            .build()
    })
}

pub fn get_relay_round_address_from_timestamp() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("getRelayRoundAddressFromTimestamp")
            .time_header()
            .in_arg("time", ton_abi::ParamType::Uint(128))
            .out_arg("value0", ton_abi::ParamType::Address)
            .build()
    })
}

pub fn prev_relay_round_end_time() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("prevRelayRoundEndTime")
            .time_header()
            .out_arg("prevRelayRoundEndTime", ton_abi::ParamType::Uint(128))
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn relay_round_initialized() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("RelayRoundInitialized")
                .in_arg("round_num", ton_abi::ParamType::Uint(128))
                .in_arg("round_start_time", ton_abi::ParamType::Uint(128))
                .in_arg("round_addr", ton_abi::ParamType::Address)
                .in_arg("relays_count", ton_abi::ParamType::Uint(128))
                .in_arg("duplicate", ton_abi::ParamType::Bool)
                .build()
        })
    }
}
