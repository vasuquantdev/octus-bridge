use nekoton_abi::*;
use once_cell::sync::OnceCell;

use crate::engine::ton_contracts::*;

/// External function
pub fn bridge_configuration() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("bridgeConfiguration")
            .default_headers()
            .out_arg("configuration", BridgeConfiguration::make_params_tuple())
            .build()
    })
}

/// External function
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

pub mod events {
    use super::*;

    pub fn connector_deployed() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("ConnectorDeployed")
                .in_arg("id", ton_abi::ParamType::Uint(64))
                .in_arg("connector", ton_abi::ParamType::Address)
                .in_arg("event_configuration", ton_abi::ParamType::Address)
                .build()
        })
    }
}
