use anyhow::Error;
use ed25519_dalek::{Keypair, PublicKey, SecretKey, Signature};
use hex::encode;
use pem::{parse_many, Pem};
use rand::prelude::*;
use rand::rngs::OsRng;
use ring::{digest, pbkdf2};
use secstr::{SecBox, SecStr, SecVec};
use serde::export::fmt::Debug;
use serde::export::Formatter;
use serde_json::from_reader;
use sha3::{Digest, Keccak256};
use sodiumoxide::crypto::aead::NONCEBYTES;
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::{Key, Nonce};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::{File, Permissions};
use std::io::{Read, Write};
use std::num::NonZeroU32;
use std::path::Path;

pub struct Signer {
    pubkey: ed25519_dalek::PublicKey,
    salt: Vec<u8>,
    encrypted_private_key: Vec<u8>,
    nonce: Nonce,
    symmetric_key: Option<Key>,
}

const CREDENTIAL_LEN: usize = digest::SHA256_OUTPUT_LEN;
const N_ITER: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(10_000_000) }; //todo tune len

//
// impl Debug for Signer {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         writeln!(f, "Pub:{:?}", self.keys.public)
//     }
// }

impl Signer {
    pub fn from_file<T>(path: T) -> Result<Self, Error>
    where
        T: AsRef<Path>,
    {
        let mut file = File::open(&path)?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        let parsed = parse_many(&content);
        let pem_map = parsed
            .into_iter()
            .fold(HashMap::new(), |mut map: HashMap<_, _>, pem| {
                map.insert(pem.tag, pem.contents);
                map
            });
        let nonce = Nonce::from_slice(pem_map.get("nonce").expect("No nonce in pem file"))
            .expect("Bad nonce provided");
        let salt = pem_map.get("salt").expect("No salt in pem file").clone();
        let pubkey = PublicKey::from_bytes(pem_map.get("pubkey").expect("No pubkey in map"))?;

        let encrypted_private_key = pem_map
            .get("encrypted_private_key")
            .expect("No encrypted_keypair in pem file")
            .clone();
        Ok(Self {
            salt,
            pubkey,
            encrypted_private_key,
            nonce,
            symmetric_key: None,
        })
    }
    fn private_key_from_encrypted(encrypted_key: &[u8], key: &Key, nonce: &Nonce) -> SecretKey {
        SecretKey::from_bytes(
            &secretbox::open(encrypted_key, nonce, key).expect("Failed decrypting SecretKey"),
        )
        .expect("Failed constructing SecretKey from decrypted data")
    }
    pub fn provide_password(self, password: SecStr) -> Self {
        const CHECK_MESSAGE:&str =  "Rust is a multi-paradigm programming language designed for performance and safety, especially safe concurrency.";
        let mut pbkdf2_hash = SecVec::new(Vec::with_capacity(CREDENTIAL_LEN));
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            N_ITER,
            &self.salt.clone(),
            password.unsecure(),
            &mut pbkdf2_hash.unsecure_mut(),
        );

        let key = secretbox::Key::from_slice(&pbkdf2_hash.unsecure()).expect("Shouldn't panic");
        let privkey:SecretKey = Self::private_key_from_encrypted(&self.encrypted_private_key, &key, &self.nonce);

        let sign = privkey.sign(CHECK_MESSAGE.as_bytes(), )
        Signer {
            symmetric_key: Some(key),
            ..self
        }
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        use ed25519_dalek::Signer;
        use sodiumoxide::crypto::secretbox;
        sodiumoxide::init();
        let nonce = secretbox::Nonce::from_slice(&nonce_bytes[..]).expect("Shouldn't panic");
        let key_pair_bytes = SecretKey::from_bytes(
            &secretbox::open(&self.encrypted_private_key, &nonce, &self.key)
                .expect("Failed decrypting keypair"),
        )
        .expect("Failed constructing secret key from encrypted secret key");

        let mut eth_data: Vec<u8> = b"\x19Ethereum Signed Message:\n".to_vec();
        eth_data.extend_from_slice(data.len().to_string().as_bytes());
        eth_data.extend_from_slice(&data);
        let hash = Keccak256::digest(&eth_data);
        let keys = Keypair::from_bytes(key_pair_bytes).expect("Bad keypair");
        keys.sign(&*hash)
    }

    pub fn init<T>(pem_file_path: T, password: SecStr) -> Result<Self, Error>
    //todo use Writer instead of Path?
    where
        T: AsRef<Path>,
    {
        sodiumoxide::init().expect("Failed initializing libsodium");
        let nonce = secretbox::gen_nonce();
        let mut rng = rand::rngs::OsRng;
        let mut salt = vec![0u8; CREDENTIAL_LEN];
        rng.fill(salt.as_mut_slice());
        let mut pbkdf2_hash = SecVec::new(Vec::with_capacity(CREDENTIAL_LEN));
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            N_ITER,
            &salt,
            password.unsecure(),
            &mut pbkdf2_hash.unsecure_mut(),
        );

        let key = secretbox::Key::from_slice(&pbkdf2_hash.unsecure()).expect("Shouldn't panic");
        pbkdf2_hash.zero_out();
        drop(pbkdf2_hash);
        let keypair = Keypair::generate(&mut rng);
        let encrypted_private_key = secretbox::seal(keypair.secret.as_bytes(), &nonce, &key);
        let pubkey = Vec::from(keypair.public.to_bytes());
        let nonce_bytes = Vec::from(nonce.0);
        drop(keypair);
        let pem_data = vec![
            Pem {
                tag: "pubkey".into(),
                contents: pubkey.clone(),
            },
            Pem {
                tag: "salt".into(),
                contents: salt.clone(),
            },
            Pem {
                tag: "encrypted_private_key".into(),
                contents: encrypted_private_key.clone(),
            },
            Pem {
                tag: "nonce".into(),
                contents: nonce_bytes,
            },
        ];
        let serialized_self = pem::encode_many(&pem_data);
        let mut pem_file = File::create(pem_file_path)?;
        pem_file.write_all(serialized_self.as_bytes())?;

        Ok(Self {
            salt: Vec::from(salt),
            nonce,
            pubkey: PublicKey::from_bytes(&*pubkey).expect("Can't fail"),
            encrypted_private_key: encrypted_private_key,
            symmetric_key: Some(key),
        })
    }
}

#[cfg(test)]
mod test {
    use crate::key_managment::Signer;
    use ed25519_dalek::ed25519::signature::Signature;
    use ed25519_dalek::Keypair;

    // #[test]
    // fn test_sign() {
    //     use hex::decode;
    //     let message_text = b"Some data";
    //
    //     let signer = Signer::from_file("./test/test.keys").unwrap();
    //     let res = signer.sign(b"test");
    //     assert_eq!(res, Signature::from_bytes(decode("2d34bd34780a6fb76181c103653161c456eb7163eb9e098b29fc1619b46fa123ae3d962ca738e480a83ed1943e965b652175c78536781abe426f46cdfe64a209").unwrap().as_slice()).unwrap());
    // }
    #[test]
    fn test_size() {
        assert_eq!(
            sodiumoxide::crypto::secretbox::xsalsa20poly1305::KEYBYTES,
            ring::digest::SHA256_OUTPUT_LEN
        );
    }
}
