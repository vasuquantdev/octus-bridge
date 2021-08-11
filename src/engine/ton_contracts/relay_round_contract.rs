use nekoton_abi::*;
use once_cell::sync::OnceCell;

pub fn relay_keys() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("relayKeys")
            .time_header()
            .out_arg(
                "value0",
                ton_abi::ParamType::Array(Box::new(ton_abi::ParamType::Uint(256))),
            )
            .build()
    })
}
