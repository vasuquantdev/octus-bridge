use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

pub fn get_details() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("getDetails")
            .time_header()
            .out_arg("id", ton_abi::ParamType::Uint(64))
            .out_arg("event_configuration", ton_abi::ParamType::Address)
            .out_arg("enabled", ton_abi::ParamType::Bool)
            .build()
    })
}
