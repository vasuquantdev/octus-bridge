use std::fs::File;
use std::path::Path;

use anyhow::Result;
use chacha20poly1305::aead::NewAead;
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use nekoton_utils::*;
use rand::prelude::*;
use secstr::SecUtf8;
use serde::{Deserialize, Serialize};

use crate::utils::*;

/// Keystore data, stored on disk
#[derive(Serialize, Deserialize)]
pub struct StoredKeysData {
    #[serde(with = "serde_bytes_base64")]
    salt: Vec<u8>,

    /// ETH part
    eth: StoredKeysDataPart,
    /// TON part
    ton: StoredKeysDataPart,
}

impl StoredKeysData {
    /// Encrypts ETH and TON data
    pub fn new(password: &str, eth: UnencryptedEthData, ton: UnencryptedTonData) -> Result<Self> {
        let mut rng = rand::rngs::OsRng;
        let salt: [u8; 20] = rng.gen();

        let key = symmetric_key_from_password(password, &salt);
        let encryptor = ChaCha20Poly1305::new(&key);

        Ok(Self {
            salt: salt.to_vec(),
            eth: eth.encrypt(&encryptor, &mut rng)?,
            ton: ton.encrypt(&encryptor, &mut rng)?,
        })
    }

    /// Decrypts full ETH and TON data
    pub fn decrypt(&self, password: &str) -> Result<(UnencryptedEthData, UnencryptedTonData)> {
        let key = symmetric_key_from_password(password, &self.salt);
        let decrypter = ChaCha20Poly1305::new(&key);

        let eth = UnencryptedEthData::decrypt(&decrypter, &self.eth)?;
        let ton = UnencryptedTonData::decrypt(&decrypter, &self.ton)?;

        Ok((eth, ton))
    }

    /// Decrypts private keys from ETH and TON data
    pub fn decrypt_only_keys(&self, password: &str) -> Result<([u8; 32], [u8; 32])> {
        let key = symmetric_key_from_password(password, &self.salt);
        let decrypter = ChaCha20Poly1305::new(&key);

        let eth = self.eth.decrypt_secret_key(&decrypter)?;
        let ton = self.ton.decrypt_secret_key(&decrypter)?;

        Ok((eth, ton))
    }

    /// Loads data from disk
    pub fn load<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path)?;
        let data: Self = serde_json::from_reader(&file)?;
        Ok(data)
    }

    /// Stores data on disk
    pub fn save<P>(&self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let file = File::create(path)?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}

/// Encrypted seed and path
#[derive(Serialize, Deserialize)]
pub struct StoredKeysDataPart {
    #[serde(with = "serde_bytes_base64")]
    pub encrypted_seed_phrase: Vec<u8>,
    #[serde(with = "serde_bytes_base64")]
    pub encrypted_derivation_path: Vec<u8>,
    #[serde(with = "serde_nonce")]
    pub nonce: Nonce,
}

impl StoredKeysDataPart {
    fn decrypt_secret_key(&self, decrypter: &ChaCha20Poly1305) -> Result<[u8; 32]> {
        let seed_phrase = decrypt_secure_str(decrypter, &self.nonce, &self.encrypted_seed_phrase)?;
        let path = decrypt_secure_str(decrypter, &self.nonce, &self.encrypted_derivation_path)?;
        derive_secret_from_phrase(seed_phrase.unsecure(), path.unsecure())
    }
}

/// Raw ETH seed phrase with derived address
pub struct UnencryptedEthData {
    phrase: SecUtf8,
    path: SecUtf8,
    address: ethabi::Address,
}

impl FromPhraseAndPath for UnencryptedEthData {
    const DEFAULT_PATH: &'static str = "m/44'/60'/0'/0/0";

    fn phrase(&self) -> &SecUtf8 {
        &self.phrase
    }

    fn path(&self) -> &SecUtf8 {
        &self.path
    }

    fn from_phrase(phrase: SecUtf8, path: SecUtf8) -> Result<Self> {
        let secret_key = secp256k1::SecretKey::from_slice(&derive_secret_from_phrase(
            phrase.unsecure(),
            path.unsecure(),
        )?)?;
        let public_key =
            secp256k1::PublicKey::from_secret_key(&secp256k1::Secp256k1::new(), &secret_key);
        let address = compute_eth_address(&public_key);

        Ok(Self {
            phrase,
            path,
            address,
        })
    }

    fn as_printable(&self) -> serde_json::Value {
        #[derive(Serialize)]
        struct PrintedUnencryptedEthData<'a> {
            phrase: &'a str,
            path: &'a str,
            address: EthAddressWrapper<'a>,
        }

        serde_json::to_value(PrintedUnencryptedEthData {
            phrase: self.phrase.unsecure(),
            path: self.path.unsecure(),
            address: EthAddressWrapper(&self.address),
        })
        .trust_me()
    }
}

/// Raw TON seed phrase with derived public key
pub struct UnencryptedTonData {
    phrase: SecUtf8,
    path: SecUtf8,
    public_key: ed25519_dalek::PublicKey,
}

impl FromPhraseAndPath for UnencryptedTonData {
    const DEFAULT_PATH: &'static str = "m/44'/396'/0'/0/0";

    fn phrase(&self) -> &SecUtf8 {
        &self.phrase
    }

    fn path(&self) -> &SecUtf8 {
        &self.path
    }

    fn from_phrase(phrase: SecUtf8, path: SecUtf8) -> Result<Self> {
        let secret_key = ed25519_dalek::SecretKey::from_bytes(&derive_secret_from_phrase(
            phrase.unsecure(),
            path.unsecure(),
        )?)?;
        let public_key = ed25519_dalek::PublicKey::from(&secret_key);

        Ok(Self {
            phrase,
            path,
            public_key,
        })
    }

    fn as_printable(&self) -> serde_json::Value {
        #[derive(Serialize)]
        struct PrintedUnencryptedTonData<'a> {
            phrase: &'a str,
            path: &'a str,
            public_key: String,
        }

        serde_json::to_value(PrintedUnencryptedTonData {
            phrase: self.phrase.unsecure(),
            path: self.path.unsecure(),
            public_key: ton_types::UInt256::from(self.public_key.to_bytes()).to_hex_string(),
        })
        .trust_me()
    }
}

pub trait FromPhraseAndPath: Sized {
    const DEFAULT_PATH: &'static str;

    fn phrase(&self) -> &SecUtf8;
    fn path(&self) -> &SecUtf8;
    fn from_phrase(phrase: SecUtf8, path: SecUtf8) -> Result<Self>;
    fn as_printable(&self) -> serde_json::Value;

    fn generate() -> Result<Self> {
        Self::from_phrase(
            SecUtf8::from(
                bip39::Mnemonic::new(bip39::MnemonicType::Words12, bip39::Language::English)
                    .into_phrase(),
            ),
            SecUtf8::from(Self::DEFAULT_PATH),
        )
    }

    fn decrypt(decrypter: &ChaCha20Poly1305, part: &StoredKeysDataPart) -> Result<Self> {
        let phrase = decrypt_secure_str(decrypter, &part.nonce, &part.encrypted_seed_phrase)?;
        let path = decrypt_secure_str(decrypter, &part.nonce, &part.encrypted_derivation_path)?;
        Self::from_phrase(phrase, path)
    }

    fn encrypt(
        &self,
        encryptor: &ChaCha20Poly1305,
        rng: &mut impl Rng,
    ) -> Result<StoredKeysDataPart> {
        let nonce = generate_nonce(rng);
        let encrypted_seed_phrase =
            encrypt(encryptor, &nonce, self.phrase().unsecure().as_bytes())?;
        let encrypted_derivation_path =
            encrypt(encryptor, &nonce, self.path().unsecure().as_bytes())?;

        Ok(StoredKeysDataPart {
            encrypted_seed_phrase,
            encrypted_derivation_path,
            nonce,
        })
    }
}

fn derive_secret_from_phrase(phrase: &str, path: &str) -> Result<[u8; 32]> {
    let mnemonic = bip39::Mnemonic::from_phrase(phrase, bip39::Language::English)?;
    let hd = bip39::Seed::new(&mnemonic, "");
    let seed_bytes = hd.as_bytes();

    let derived = tiny_hderive::bip32::ExtendedPrivKey::derive(seed_bytes, path)
        .map_err(|_| StoredKeysError::KeyDerivationFailed)?;
    Ok(derived.secret())
}

fn generate_nonce(rng: &mut impl rand::Rng) -> Nonce {
    use chacha20poly1305::aead::generic_array::sequence::GenericSequence;
    Nonce::generate(|_| rng.gen())
}

#[derive(thiserror::Error, Debug)]
enum StoredKeysError {
    #[error("Failed to derive key from mnemonic")]
    KeyDerivationFailed,
}
