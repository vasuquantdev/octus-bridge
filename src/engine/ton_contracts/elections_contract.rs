use nekoton_abi::*;

use super::models::*;

pub fn staker_addrs() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("staker_addrs")
            .time_header()
            .expire_header()
            .outputs(StakerAddresses::param_type())
            .build()
    })
}
