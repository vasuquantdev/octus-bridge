/// Get address according to https://github.com/ethereumbook/ethereumbook/blob/develop/04keys-addresses.asciidoc#public-keys
pub fn compute_eth_address(public_key: &secp256k1::PublicKey) -> ethabi::Address {
    let pub_key = &public_key.serialize_uncompressed()[1..];
    ethabi::Address::from_slice(&web3::signing::keccak256(pub_key)[32 - 20..])
}
