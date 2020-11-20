use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Error;
use base64::{decode, encode};
use ed25519_dalek::{ed25519, Keypair, Signer};
use rand::prelude::*;
use ring::{digest, pbkdf2};
use secp256k1::{Message, PublicKey, SecretKey, Signature};
use secstr::{SecStr, SecVec};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{from_reader, to_writer_pretty};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::{Key, Nonce};

const CREDENTIAL_LEN: usize = digest::SHA256_OUTPUT_LEN;

///Change it to tune number of iterations in pbkdf2 function. Higher number - password bruteforce becomes slower.
/// Initial value is optimal for the current machine, so you maybe want to change it.
const N_ITER: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(5_000_000) };

#[derive(Eq, PartialEq, Debug)]
pub struct KeyData {
    pub eth: EthSigner,
    pub ton: TonSigner,
}

#[derive(Eq, PartialEq)]
pub struct EthSigner {
    pubkey: PublicKey,
    private_key: SecretKey,
}

#[derive(Clone)]
pub struct TonSigner {
    inner: Arc<Keypair>,
}

impl Eq for TonSigner {}

impl PartialEq for TonSigner {
    fn eq(&self, other: &Self) -> bool {
        self.inner
            .secret
            .as_bytes()
            .eq(other.inner.secret.as_bytes())
    }
}

impl Debug for TonSigner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.inner.public)
    }
}

impl Debug for EthSigner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.pubkey)
    }
}

#[derive(Serialize, Deserialize)]
struct CryptoData {
    #[serde(serialize_with = "buffer_to_hex", deserialize_with = "hex_to_buffer")]
    salt: Vec<u8>,

    #[serde(
        serialize_with = "serialize_pubkey",
        deserialize_with = "deserialize_pubkey"
    )]
    eth_pubkey: PublicKey,
    #[serde(serialize_with = "buffer_to_hex", deserialize_with = "hex_to_buffer")]
    eth_encrypted_private_key: Vec<u8>,
    #[serde(
        serialize_with = "serialize_nonce",
        deserialize_with = "deserialize_nonce"
    )]
    eth_nonce: Nonce,

    #[serde(serialize_with = "buffer_to_hex", deserialize_with = "hex_to_buffer")]
    ton_encrypted_private_key: Vec<u8>,
    #[serde(
        serialize_with = "serialize_nonce",
        deserialize_with = "deserialize_nonce"
    )]
    ton_nonce: Nonce,
}

/// Serializes `buffer` to a lowercase hex string.
pub fn buffer_to_hex<T, S>(buffer: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: AsRef<[u8]> + ?Sized,
    S: Serializer,
{
    serializer.serialize_str(&*encode(&buffer.as_ref()))
}

/// Deserializes a lowercase hex string to a `Vec<u8>`.
pub fn hex_to_buffer<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    String::deserialize(deserializer)
        .and_then(|string| decode(string).map_err(|e| D::Error::custom(e.to_string())))
}

fn serialize_pubkey<S>(t: &PublicKey, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    buffer_to_hex(&t.serialize(), ser)
}

fn serialize_nonce<S>(t: &Nonce, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    buffer_to_hex(&t[..], ser)
}

fn deserialize_nonce<'de, D>(deser: D) -> Result<Nonce, D::Error>
where
    D: Deserializer<'de>,
{
    hex_to_buffer(deser).and_then(|x| {
        Nonce::from_slice(&*x).ok_or_else(|| serde::de::Error::custom("Failed deserializing nonce"))
    })
}

fn deserialize_pubkey<'de, D>(deser: D) -> Result<PublicKey, D::Error>
where
    D: Deserializer<'de>,
{
    hex_to_buffer(deser).and_then(|x| {
        PublicKey::from_slice(&*x).map_err(|e| serde::de::Error::custom(e.to_string()))
    })
}

impl EthSigner {
    pub fn sign(&self, data: &[u8]) -> Result<Signature, Error> {
        use sha3::{Digest, Keccak256};
        let mut eth_data: Vec<u8> = b"\x19Ethereum Signed Message:\n".to_vec();
        eth_data.extend_from_slice(data.len().to_string().as_bytes());
        eth_data.extend_from_slice(&data);
        let hash = Keccak256::digest(&eth_data);
        let message = Message::from_slice(&*hash)?;
        let secp = secp256k1::Secp256k1::new();
        Ok(secp.sign(&message, &self.private_key))
    }
}

impl TonSigner {
    pub fn public_key(&self) -> &[u8; 32] {
        self.inner.public.as_bytes()
    }

    pub fn sign(&self, data: &[u8]) -> [u8; ed25519::SIGNATURE_LENGTH] {
        self.inner.sign(data).to_bytes()
    }

    pub fn keypair(&self) -> Arc<ed25519_dalek::Keypair> {
        self.inner.clone()
    }
}

impl KeyData {
    pub fn from_file<T>(path: T, password: SecStr) -> Result<Self, Error>
    where
        T: AsRef<Path>,
    {
        let file = File::open(&path)?;
        let crypto_data: CryptoData = from_reader(&file)?;
        let sym_key = Self::symmetric_key_from_password(password, &*crypto_data.salt);

        let eth_private_key = Self::eth_private_key_from_encrypted(
            &crypto_data.eth_encrypted_private_key,
            &sym_key,
            &crypto_data.eth_nonce,
        )?;

        let ton_data = Self::ton_private_key_from_encrypted(
            &crypto_data.ton_encrypted_private_key,
            &sym_key,
            &crypto_data.ton_nonce,
        )?;

        Ok(Self {
            eth: EthSigner {
                pubkey: crypto_data.eth_pubkey,
                private_key: eth_private_key,
            },
            ton: TonSigner {
                inner: Arc::new(ton_data),
            },
        })
    }

    pub fn init<T>(
        pem_file_path: T,
        password: SecStr,
        eth_private_key: SecretKey,
        ton_key_pair: ed25519_dalek::Keypair,
    ) -> Result<Self, Error>
    //todo use Writer instead of Path?
    where
        T: AsRef<Path>,
    {
        sodiumoxide::init().expect("Failed initializing libsodium");

        let mut rng = rand::rngs::OsRng::new().expect("OsRng fail");
        let mut salt = vec![0u8; CREDENTIAL_LEN];
        rng.fill(salt.as_mut_slice());
        let key = Self::symmetric_key_from_password(password, &salt);

        // ETH
        let (eth_pubkey, eth_encrypted_private_key, eth_nonce) = {
            let curve = secp256k1::Secp256k1::new();

            let public = PublicKey::from_secret_key(&curve, &eth_private_key);
            let nonce = secretbox::gen_nonce();
            let private_key = secretbox::seal(&eth_private_key[..], &nonce, &key);
            (public, private_key, nonce)
        };

        // TON
        let (ton_encrypted_private_key, ton_nonce) = {
            let nonce = secretbox::gen_nonce();
            let private_key = secretbox::seal(ton_key_pair.secret.as_bytes(), &nonce, &key);
            (private_key, nonce)
        };

        //
        let data = CryptoData {
            salt,
            eth_pubkey,
            eth_encrypted_private_key,
            eth_nonce,
            ton_encrypted_private_key,
            ton_nonce,
        };

        let crypto_config = File::create(pem_file_path)?;
        to_writer_pretty(crypto_config, &data)?;
        Ok(Self {
            eth: EthSigner {
                private_key: eth_private_key,
                pubkey: eth_pubkey,
            },
            ton: TonSigner {
                inner: Arc::new(ton_key_pair),
            },
        })
    }

    fn symmetric_key_from_password(password: SecStr, salt: &[u8]) -> Key {
        let mut pbkdf2_hash = SecVec::new(vec![0; CREDENTIAL_LEN]);
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            N_ITER,
            salt,
            password.unsecure(),
            &mut pbkdf2_hash.unsecure_mut(),
        );
        secretbox::Key::from_slice(&pbkdf2_hash.unsecure()).expect("Shouldn't panic")
    }

    fn eth_private_key_from_encrypted(
        encrypted_key: &[u8],
        key: &Key,
        nonce: &Nonce,
    ) -> Result<SecretKey, Error> {
        SecretKey::from_slice(
            &secretbox::open(encrypted_key, nonce, key)
                .map_err(|_| anyhow!("Failed decrypting eth SecretKey"))?,
        )
        .map_err(|_| anyhow!("Failed constructing SecretKey from decrypted data"))
    }

    fn ton_private_key_from_encrypted(
        encrypted_key: &[u8],
        key: &Key,
        nonce: &Nonce,
    ) -> Result<ed25519_dalek::Keypair, Error> {
        secretbox::open(encrypted_key, nonce, key)
            .map_err(|_| anyhow!("Failed decrypting with provided password"))
            .and_then(|data| {
                Keypair::from_bytes(&data)
                    .map_err(|e| anyhow!("failed to load ton key. {}", e.to_string()))
            })
    }
}

#[cfg(test)]
mod test {
    use secp256k1::SecretKey;
    use secstr::SecStr;

    use crate::crypto::key_managment::KeyData;

    fn default_keys() -> (SecretKey, ed25519_dalek::Keypair) {
        let eth_private_key = SecretKey::from_slice(&hex::decode("9ee05332323beff8b0f27bc09d7be149c8387a32d392eb0dceffba58a23b9e8d3d1a07db8b045e784ea44097430ea4faac23b46e3d709192d23ea6fbfb53ad07")
            .unwrap()).unwrap();

        let ton_private_key = ed25519_dalek::SecretKey::from_bytes(
            &hex::decode("e371ef1d7266fc47b30d49dc886861598f09e2e6294d7f0520fe9aa460114e51")
                .unwrap(),
        )
        .unwrap();
        let ton_public_key = ed25519_dalek::PublicKey::from(&ton_private_key);
        let ton_key_pair = ed25519_dalek::Keypair {
            secret: ton_private_key,
            public: ton_public_key,
        };

        (eth_private_key, ton_key_pair)
    }

    // #[test]
    // fn test_sign() {
    //     use hex::decode;
    //     let message_text = b"Some data";
    //
    //     let signer = Signer::from_file("./test/test.keys")
    //     let res = signer.sign(b"test").unwrap();
    //     assert_eq!(res, Signature::from_bytes(decode("2d34bd34780a6fb76181c103653161c456eb7163eb9e098b29fc1619b46fa123ae3d962ca738e480a83ed1943e965b652175c78536781abe426f46cdfe64a209").unwrap().as_slice()).unwrap());
    // }
    #[test]
    fn test_init() {
        let private = SecretKey::from_slice(
            &hex::decode("416ddb82736d0ddf80cc50eda0639a2dd9f104aef121fb9c8af647ad8944a8b1")
                .unwrap(),
        )
        .unwrap();
        let password = SecStr::new("123".into());
        let path = "./test/test_init.key";

        let (eth_private_key, ton_key_pair) = default_keys();

        let signer = KeyData::init(&path, password.clone(), eth_private_key, ton_key_pair).unwrap();
        let read_signer = KeyData::from_file(&path, password).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(read_signer, signer);
    }

    #[test]
    fn test_bad_password() {
        let password = SecStr::new("123".into());
        let path = "./test/test_bad.key";
        let private = SecretKey::from_slice(
            &hex::decode("416ddb82736d0ddf80cc50eda0639a2dd9f104aef121fb9c8af647ad8944a8b1")
                .unwrap(),
        )
        .unwrap();

        let (eth_private_key, ton_key_pair) = default_keys();

        KeyData::init(&path, password.clone(), eth_private_key, ton_key_pair).unwrap();
        let result = KeyData::from_file(&path, SecStr::new("lol".into()));
        std::fs::remove_file(path).unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_size() {
        assert_eq!(
            sodiumoxide::crypto::secretbox::xsalsa20poly1305::KEYBYTES,
            ring::digest::SHA256_OUTPUT_LEN
        );
    }
}
