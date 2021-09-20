use nekoton_abi::*;
use once_cell::sync::OnceCell;

use super::models::*;

pub fn staker_addrs() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("staker_addrs")
            .time_header()
            .expire_header()
            .outputs(StakerAddresses::param_type())
            .build()
    })
}
