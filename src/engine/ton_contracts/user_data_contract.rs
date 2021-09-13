use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

pub fn confirm_ton_account() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("confirmTonAccount")
            .default_headers()
            .build()
    })
}

pub fn get_details() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getDetails")
            .default_headers()
            .output("details", UserDataDetails::param_type())
            .build()
    })
}
pub fn become_relay_next_round() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("becomeRelayNextRound")
            .default_headers()
            .build()
    })
}

pub fn get_reward_for_relay_round() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("getRewardForRelayRound")
            .default_headers()
            .input("round_num", u32::param_type())
            .build()
    })
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
