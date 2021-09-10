use nekoton_abi::*;
use once_cell::sync::OnceCell;

/// External function
pub fn confirm() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| {
        FunctionBuilder::new("confirm")
            .default_headers()
            .input("signature", ton_abi::ParamType::Bytes)
            .build()
    })
}

/// External function
pub fn reject() -> &'static ton_abi::Function {
    static FUNCTION: OnceCell<ton_abi::Function> = OnceCell::new();
    FUNCTION.get_or_init(|| FunctionBuilder::new("reject").default_headers().build())
}
