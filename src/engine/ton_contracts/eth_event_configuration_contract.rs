use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

pub fn get_details() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getDetails")
            .time_header()
            .expire_header()
            .out_arg(
                "basic_configuration",
                BasicConfiguration::make_params_tuple(),
            )
            .out_arg(
                "network_configuration",
                EthEventConfiguration::make_params_tuple(),
            )
            .build()
    })
}
