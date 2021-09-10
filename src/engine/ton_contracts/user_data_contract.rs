use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

pub fn confirm_ton_account() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| FunctionBuilder::new("confirmTonAccount").build())
}

pub fn become_relay_next_round() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| FunctionBuilder::new("becomeRelayNextRound").build())
}

pub mod events {
    use super::*;

    pub fn relay_membership_requested() -> &'static ton_abi::Event {
        static FUNCTION: OnceCell<ton_abi::Event> = OnceCell::new();
        FUNCTION.get_or_init(|| {
            EventBuilder::new("RelayMembershipRequested")
                .inputs(RelayMembershipRequestedEvent::param_type())
                .build()
        })
    }
}
