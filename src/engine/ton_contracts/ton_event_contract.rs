use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

pub fn get_details() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getDetails")
            .default_headers()
            .out_arg("event_init_data", TonEventInitData::make_params_tuple())
            .out_arg("status", ton_abi::ParamType::Uint(8))
            .out_arg(
                "confirms",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .out_arg(
                "rejects",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .out_arg(
                "empty",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .out_arg(
                "signatures",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Bytes)),
            )
            .out_arg("balance", ton_abi::ParamType::Uint(128))
            .out_arg("initializer", ton_abi::ParamType::Address)
            .out_arg("meta", ton_abi::ParamType::Cell)
            .out_arg("required_votes", ton_abi::ParamType::Uint(32))
            .build()
    })
}
