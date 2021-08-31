use std::fs::File;
use std::path::Path;

use anyhow::Result;
use chacha20poly1305::aead::NewAead;
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use nekoton_utils::*;
use rand::prelude::*;
use serde::{Deserialize, Serialize};

pub struct KeyStore {
    pub eth: EthSigner,
    pub ton: TonSigner,
}

impl KeyStore {
    pub fn from_file<T>(path: T, password: &str) -> Result<Self>
    where
        T: AsRef<Path>,
    {
        let file = File::open(&path)?;
        let crypto_data: StoredData = serde_json::from_reader(&file)?;

        let key = symmetric_key_from_password(password, &*crypto_data.salt);
        let decrypter = ChaCha20Poly1305::new(&key);

        let eth_secret_key = secp256k1::SecretKey::from_slice(&decrypt(
            &decrypter,
            &crypto_data.eth_nonce,
            &crypto_data.eth_encrypted_secret_key,
        )?)?;

        let ton_secret_key = ed25519_dalek::SecretKey::from_bytes(&decrypt(
            &decrypter,
            &crypto_data.ton_nonce,
            &crypto_data.ton_encrypted_secret_key,
        )?)?;

        Ok(Self {
            eth: EthSigner::new(eth_secret_key),
            ton: TonSigner::new(ton_secret_key),
        })
    }
}

pub struct EthSigner {
    secp256k1: secp256k1::Secp256k1<secp256k1::All>,
    secret_key: secp256k1::SecretKey,
    public_key: secp256k1::PublicKey,
    address: ethabi::Address,
}

impl EthSigner {
    fn new(secret_key: secp256k1::SecretKey) -> Self {
        let secp256k1 = secp256k1::Secp256k1::new();
        let public_key = secp256k1::PublicKey::from_secret_key(&secp256k1, &secret_key);

        // Get address according to https://github.com/ethereumbook/ethereumbook/blob/develop/04keys-addresses.asciidoc#public-keys
        let address = {
            let pub_key = &public_key.serialize_uncompressed()[1..];
            ethabi::Address::from_slice(&web3::signing::keccak256(pub_key)[32 - 20..])
        };

        Self {
            secp256k1,
            secret_key,
            public_key,
            address,
        }
    }

    /// signs data according to https://eips.ethereum.org/EIPS/eip-191
    pub fn sign(&self, data: &[u8]) -> Vec<u8> {
        // 1. Calculate prefixed hash
        let data_hash = web3::signing::keccak256(data);
        let mut eth_data: Vec<u8> = "\x19Ethereum Signed Message:\n32".into();
        eth_data.extend_from_slice(&data_hash);

        // 2. Calculate hash of prefixed hash
        let hash = web3::signing::keccak256(&eth_data);
        let message = secp256k1::Message::from_slice(&hash).expect("Shouldn't fail");

        // 3. Sign
        let (id, signature) = self
            .secp256k1
            .sign_recoverable(&message, &self.secret_key)
            .serialize_compact();

        // 4. Prepare for ETH
        let mut ex_sign = Vec::with_capacity(65);
        ex_sign.extend_from_slice(&signature);
        ex_sign.push(id.to_i32() as u8 + 27); // recovery id with eth specific offset

        // Done
        ex_sign
    }

    pub fn pubkey(&self) -> &secp256k1::PublicKey {
        &self.public_key
    }

    pub fn address(&self) -> &ethabi::Address {
        &self.address
    }
}

pub struct TonSigner {
    pair: ed25519_dalek::Keypair,
}

impl TonSigner {
    fn new(secret_key: ed25519_dalek::SecretKey) -> Self {
        let public_key = ed25519_dalek::PublicKey::from(&secret_key);

        Self {
            pair: ed25519_dalek::Keypair {
                secret: secret_key,
                public: public_key,
            },
        }
    }

    pub fn public_key(&self) -> &ed25519_dalek::PublicKey {
        &self.pair.public
    }

    pub fn sign(&self, data: &[u8]) -> [u8; ed25519_dalek::SIGNATURE_LENGTH] {
        use ed25519_dalek::Signer;

        self.pair.sign(data).to_bytes()
    }
}

/// Data, stored on disk in `encrypted_data` filed of config.
#[derive(Serialize, Deserialize)]
pub struct StoredData {
    #[serde(with = "serde_bytes_base64")]
    salt: Vec<u8>,

    #[serde(with = "serde_bytes_base64")]
    eth_encrypted_secret_key: Vec<u8>,
    #[serde(with = "serde_nonce")]
    eth_nonce: Nonce,

    #[serde(with = "serde_bytes_base64")]
    ton_encrypted_secret_key: Vec<u8>,
    #[serde(with = "serde_nonce")]
    ton_nonce: Nonce,
}

impl StoredData {
    pub fn new(
        password: &str,
        eth_secret_key: secp256k1::SecretKey,
        ton_secret_key: ed25519_dalek::SecretKey,
    ) -> Result<Self> {
        fn gen_part(
            enc: &ChaCha20Poly1305,
            rng: &mut impl Rng,
            data: &[u8],
        ) -> Result<(Vec<u8>, Nonce)> {
            use chacha20poly1305::aead::generic_array::sequence::GenericSequence;

            let nonce = Nonce::generate(|_| rng.gen());
            let data = encrypt(enc, &nonce, data)?;
            Ok((data, nonce))
        }

        let mut rng = rand::rngs::OsRng;

        let salt: [u8; 20] = rng.gen();

        let key = symmetric_key_from_password(password, &salt);
        let encryptor = ChaCha20Poly1305::new(&key);

        // Encrypt ETH part
        let (eth_encrypted_secret_key, eth_nonce) =
            gen_part(&encryptor, &mut rng, eth_secret_key.as_ref())?;

        // Encrypt TON part
        let (ton_encrypted_secret_key, ton_nonce) =
            gen_part(&encryptor, &mut rng, ton_secret_key.as_bytes())?;

        // Done
        Ok(Self {
            salt: salt.to_vec(),
            eth_encrypted_secret_key,
            eth_nonce,
            ton_encrypted_secret_key,
            ton_nonce,
        })
    }

    pub fn save<T>(&self, path: T) -> Result<()>
    where
        T: AsRef<Path>,
    {
        let crypto_config = File::create(path)?;
        serde_json::to_writer_pretty(crypto_config, self)?;
        Ok(())
    }
}

#[cfg(test)]
mod tst {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use std::str::FromStr;
    use tempfile::TempDir;

    #[test]
    fn init() {
        let (dir, path) = create_file();
        let ton = ed25519_dalek::SecretKey::from_bytes(&[0; 32]).unwrap();
        let eth = secp256k1::SecretKey::from_slice(&[1; 32]).unwrap();
        let data = StoredData::new("lol", eth, ton).unwrap();
        data.save(path).unwrap();
    }

    const JSON: &str = r#"{
        "salt": "chBVmBqLz5SMqmhjIInNCNpO48E=",
        "eth_encrypted_secret_key": "9pC/Z0iyqH0nbW7fsht62+5bRqjApRg3zjhgy/P7In/rTZ4+3IaDdB0Wtr4zcJJP",
        "eth_nonce": "ed88d2fe388cd5e59a674d6f",
        "ton_encrypted_secret_key": "XKRZpapEnk07jTkxhzVTYsUaFF4tRwH+InUoxWhjZ50tv8wlhmR1d3GE7KSNpN35",
        "ton_nonce": "90f70cfc10ffdce3b390b5fd"
    }"#;

    #[test]
    fn check_ok_passwd() {
        let (dir, path) = create_file();
        let store = KeyStore::from_file(path, "lol").unwrap();
        assert_eq!(store.ton.pair.secret.to_bytes(), [0; 32]);
        assert_eq!(store.eth.secret_key.as_ref(), &[1; 32]);
    }

    #[test]
    fn check_bad_password() {
        let (dir, path) = create_file();
        assert!(KeyStore::from_file(path, "kek").is_err())
    }

    fn create_file() -> (TempDir, PathBuf) {
        let mut dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.json");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.write_all(JSON.as_bytes()).unwrap();
        (dir, path)
    }

    fn default_keys() -> (secp256k1::SecretKey, ed25519_dalek::SecretKey) {
        let eth_private_key = secp256k1::SecretKey::from_slice(
            &hex::decode("416ddb82736d0ddf80cc50eda0639a2dd9f104aef121fb9c8af647ad8944a8b1")
                .unwrap(),
        )
        .unwrap();

        let ton_private_key = ed25519_dalek::SecretKey::from_bytes(
            &hex::decode("e371ef1d7266fc47b30d49dc886861598f09e2e6294d7f0520fe9aa460114e51")
                .unwrap(),
        )
        .unwrap();

        (eth_private_key, ton_private_key)
    }

    #[test]
    fn test_sign() {
        let message_text = b"hello_world1";

        let (private_key, _) = default_keys();
        let curve = secp256k1::Secp256k1::new();
        let signer = EthSigner::new(private_key);
        let res = signer.sign(message_text);
        let expected = hex::decode("ff244ad5573d02bc6ead270d5ff48c490b0113225dd61617791ba6610ed1e56a007ec790f8fca53243907b888e6b33ad15c52fed3bc6a7ee5da2fa287ea4f8211b").unwrap();
        assert_eq!(expected.len(), res.len());
        assert_eq!(res, expected.as_slice());
    }

    #[test]
    fn test_address_derive() {
        let (key, _) = default_keys();
        let signer = EthSigner::new(key);
        let address = signer.address();
        let expected =
            ethabi::Address::from_str("9c5a095ae311cad1b09bc36ac8635f4ed4765dcf").unwrap();
        assert_eq!(address, &expected);
    }
}
