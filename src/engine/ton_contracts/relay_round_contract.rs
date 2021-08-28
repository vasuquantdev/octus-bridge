use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

pub fn relay_keys() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("relayKeys")
            .time_header()
            .outputs(RelayKeys::param_type())
            .build()
    })
}
