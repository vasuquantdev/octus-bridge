use nekoton_abi::*;
use once_cell::sync::OnceCell;

pub fn get_type() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getType")
            .time_header()
            .expire_header()
            .output("type", ton_abi::ParamType::Uint(8))
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn new_event_contract() -> &'static ton_abi::Event {
        static FUNCTION: OnceCell<ton_abi::Event> = OnceCell::new();
        FUNCTION.get_or_init(|| {
            EventBuilder::new("NewEventContract")
                .input("address", ton_abi::ParamType::Address)
                .build()
        })
    }
}
