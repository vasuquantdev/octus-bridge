use nekoton_abi::*;

pub fn get_type() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new_responsible("getType")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .time_header()
            .expire_header()
            .output("type", ton_abi::ParamType::Uint(8))
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn new_event_contract() -> &'static ton_abi::Event {
        crate::once!(ton_abi::Event, || {
            EventBuilder::new("NewEventContract")
                .abi_version(ton_abi::contract::ABI_VERSION_2_2)
                .input("address", ton_abi::ParamType::Address)
                .build()
        })
    }
}
