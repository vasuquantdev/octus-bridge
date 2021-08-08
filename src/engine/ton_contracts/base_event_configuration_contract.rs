use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

pub fn get_type() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getType")
            .time_header()
            .expire_header()
            .out_arg("type", ton_abi::ParamType::Uint(8))
            .build()
    })
}
