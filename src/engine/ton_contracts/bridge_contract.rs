use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

pub fn derive_connector_address() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("deriveConnectorAddress")
            .default_headers()
            .in_arg("id", ton_abi::ParamType::Uint(64))
            .out_arg("connector", ton_abi::ParamType::Address)
            .build()
    })
}
