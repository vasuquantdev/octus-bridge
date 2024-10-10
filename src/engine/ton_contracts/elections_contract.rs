use nekoton_abi::*;

use super::{models::*, TON_ABI_VERSION};

pub fn staker_addrs() -> &'static ton_abi::Function {
    crate::once!(ton_abi::Function, || {
        FunctionBuilder::new("staker_addrs")
            .abi_version(TON_ABI_VERSION)
            .time_header()
            .expire_header()
            .outputs(StakerAddresses::param_type())
            .build()
    })
}
