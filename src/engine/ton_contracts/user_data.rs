use nekoton_abi::FunctionBuilder;
use once_cell::sync::OnceCell;
use ton_abi::{Function, ParamType};

pub fn relay_membership_requested_event() -> &'static Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("RelayMembershipRequestedEvent")
            .input("round_num", ParamType::Uint(32))
            .input("tokens", ParamType::Uint(128))
            .input("tokens", ParamType::Uint(256))
            .input("eth_address", ParamType::Uint(256))
            .input("lock_until", ParamType::Uint(32))
            .build()
    })
}

pub fn become_relay_next_round() -> &'static Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| FunctionBuilder::new("becomeRelayNextRound").build())
}
