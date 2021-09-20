use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

/// External function
pub fn connector_counter() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("connectorCounter")
            .default_headers()
            .output("counter", ton_abi::ParamType::Uint(64))
            .build()
    })
}

/// External responsible function
pub fn get_details() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new_responsible("getDetails")
            .default_headers()
            .outputs(BridgeDetails::param_type())
            .build()
    })
}

/// External function
pub fn derive_connector_address() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("deriveConnectorAddress")
            .default_headers()
            .input("id", ton_abi::ParamType::Uint(64))
            .output("connector", ton_abi::ParamType::Address)
            .build()
    })
}

pub mod events {
    use super::*;

    pub fn connector_deployed() -> &'static ton_abi::Event {
        static EVENT: OnceCell<ton_abi::Event> = OnceCell::new();
        EVENT.get_or_init(|| {
            EventBuilder::new("ConnectorDeployed")
                .inputs(ConnectorDeployedEvent::param_type())
                .build()
        })
    }
}
