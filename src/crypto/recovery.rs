use anyhow::Error;
use bip39::{Language, Seed};
use secp256k1::SecretKey;
use tiny_hderive::bip32::ExtendedPrivKey;

pub fn derive_from_words_eth(lang: Language, phrase: &str) -> Result<SecretKey, Error> {
    let mnemonic = bip39::Mnemonic::from_phrase(phrase, lang)?;
    let hd = Seed::new(&mnemonic, "");
    let seed_bytes = hd.as_bytes();

    let path = "m/44'/0'/0'/0/0"; //We are using default btc derive path, but it doesn't matter, because we don't use derive features.
    let derived =
        ExtendedPrivKey::derive(seed_bytes, path).map_err(|e| Error::msg(format!("{:#?}", e)))?;
    Ok(SecretKey::from_slice(&derived.secret())?)
}

pub fn derive_from_words_ton(
    lang: Language,
    phrase: &str,
    derivation_path: Option<&str>,
) -> Result<ed25519_dalek::Keypair, Error> {
    let mnemonic = bip39::Mnemonic::from_phrase(phrase, lang)?;
    let hd = Seed::new(&mnemonic, "");
    let seed_bytes = hd.as_bytes();

    let path = derivation_path.unwrap_or("m/44'/396'/0'/0/0");
    let derived =
        ExtendedPrivKey::derive(seed_bytes, path).map_err(|e| Error::msg(format!("{:#?}", e)))?;

    ed25519_keys_from_secret_bytes(&derived.secret())
}

fn ed25519_keys_from_secret_bytes(bytes: &[u8]) -> Result<ed25519_dalek::Keypair, Error> {
    let secret = ed25519_dalek::SecretKey::from_bytes(bytes).map_err(|e| {
        Error::msg(format!(
            "failed to import ton secret key. {}",
            e.to_string()
        ))
    })?;

    let public = ed25519_dalek::PublicKey::from(&secret);

    Ok(ed25519_dalek::Keypair { secret, public })
}

#[cfg(test)]
mod test {
    use bip39::Language;
    use secp256k1::SecretKey;

    use crate::crypto::recovery::*;

    #[test]
    fn test_recovery() {
        let key = derive_from_words_eth(Language::English, "talk pave choice void clever tired humor marble clutch ankle fish type deliver witness picnic thumb away offer legend keep trouble island earn pet");
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
        let key = derive_from_words_eth(Language::English, "talk rave choice void clever tired humor marble clutch ankle fish type deliver witness picnic thumb away offer legend keep trouble island earn pet");
        assert!(key.is_err());
    }

    #[test]
    fn ton_recovery() {
        let key = derive_from_words_ton(
            Language::English,
            "pioneer fever hazard scan install wise reform corn bubble leisure amazing note",
            None,
        )
        .unwrap();
        let secret = key.secret;

        let target_secret = ed25519_dalek::SecretKey::from_bytes(
            &hex::decode("e371ef1d7266fc47b30d49dc886861598f09e2e6294d7f0520fe9aa460114e51")
                .unwrap(),
        )
        .unwrap();

        assert_eq!(secret.as_bytes(), target_secret.as_bytes())
    }
}
