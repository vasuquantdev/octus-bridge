use nekoton_abi::*;

use super::TON_ABI_VERSION;

pub fn get_type() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getType")
            .abi_version(TON_ABI_VERSION)
            .time_header()
            .expire_header()
            .output("type", ton_abi::ParamType::Uint(8))
            .build()
    })
}

pub fn get_flags() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getFlags")
            .abi_version(TON_ABI_VERSION)
            .time_header()
            .expire_header()
            .output("flags", ton_abi::ParamType::Uint(64))
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn new_event_contract() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("NewEventContract")
                .abi_version(TON_ABI_VERSION)
                .input("address", ton_abi::ParamType::Address)
                .build()
        })
    }
}
