use anyhow::Error;
use bip39::{Language, Seed};
use secp256k1::SecretKey;
use tiny_hderive::bip32::ExtendedPrivKey;

pub fn derive_from_words(lang: Language, phrase: &str) -> Result<SecretKey, Error> {
    let mnemonic = bip39::Mnemonic::from_phrase(phrase, lang)?;
    let hd = Seed::new(&mnemonic, "");
    let seed_bytes = hd.as_bytes();

    let path = "m/44'/0'/0'/0/0"; //We are using default btc derive path, but it doesn't matter, because we don't use derive features.
    let derived =
        ExtendedPrivKey::derive(seed_bytes, path).map_err(|e| Error::msg(format!("{:#?}", e)))?;
    Ok(SecretKey::from_slice(&derived.secret())?)
}

#[cfg(test)]
mod test {
    use bip39::Language;
    use secp256k1::SecretKey;

    use crate::crypto::recovery::derive_from_words;

    #[test]
    fn test_recovery() {
        let key = derive_from_words(Language::English, "talk pave choice void clever tired humor marble clutch ankle fish type deliver witness picnic thumb away offer legend keep trouble island earn pet");
        assert_eq!(
            key.unwrap(),
            SecretKey::from_slice(
                &hex::decode("416ddb82736d0ddf80cc50eda0639a2dd9f104aef121fb9c8af647ad8944a8b1")
                    .unwrap(),
            )
            .unwrap(),
        );
    }
    #[test]
    fn bad_mnemonic() {
        let key = derive_from_words(Language::English, "talk rave choice void clever tired humor marble clutch ankle fish type deliver witness picnic thumb away offer legend keep trouble island earn pet");
        assert!(key.is_err());
    }
}
