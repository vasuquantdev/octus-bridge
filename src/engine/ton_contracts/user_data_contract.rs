use nekoton_abi::*;

use super::models::*;

pub fn confirm_ton_account() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("confirmTonAccount")
            .default_headers()
            .build()
    })
}

pub fn get_details() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getDetails")
            .default_headers()
            .output("details", UserDataDetails::param_type())
            .build()
    })
}
pub fn become_relay_next_round() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("becomeRelayNextRound")
            .default_headers()
            .build()
    })
}

pub fn get_reward_for_relay_round() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("getRewardForRelayRound")
            .default_headers()
            .input("round_num", u32::param_type())
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn relay_keys_updated() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("RelayKeysUpdated")
                .inputs(RelayKeysUpdatedEvent::param_type())
                .build()
        })
    }

    pub fn ton_pubkey_confirmed() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("TonPubkeyConfirmed")
                .inputs(TonPubkeyConfirmedEvent::param_type())
                .build()
        })
    }

    pub fn eth_address_confirmed() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("EthAddressConfirmed")
                .inputs(EthAddressConfirmedEvent::param_type())
                .build()
        })
    }

    pub fn relay_membership_requested() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("RelayMembershipRequested")
                .inputs(RelayMembershipRequestedEvent::param_type())
                .build()
        })
    }

    pub fn deposit_processed() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("DepositProcessed")
                .inputs(DepositProcessedEvent::param_type())
                .build()
        })
    }
}
