/// Get address according to https://github.com/ethereumbook/ethereumbook/blob/develop/04keys-addresses.asciidoc#public-keys
pub fn compute_eth_address(public_key: &secp256k1::PublicKey) -> ethabi::Address {
    let pub_key = &public_key.serialize_uncompressed()[1..];
    ethabi::Address::from_slice(&web3::signing::keccak256(pub_key)[32 - 20..])
}

pub struct EthAddressWrapper<'a>(pub &'a ethabi::Address);

impl std::fmt::Display for EthAddressWrapper<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("0x{}", hex::encode(self.0.as_bytes())))
    }
}

impl serde::Serialize for EthAddressWrapper<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
