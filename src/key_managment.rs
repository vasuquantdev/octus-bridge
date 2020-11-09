use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{Read, Write};
use std::num::NonZeroU32;
use std::path::Path;

use anyhow::Error;
use pem::{parse_many, Pem};
use rand::prelude::*;
use ring::{digest, pbkdf2};
use secp256k1::{Message, PublicKey, SecretKey, Signature};
use secstr::{SecStr, SecVec};
use sha3::{Digest, Keccak256};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::{Key, Nonce};

#[derive(Eq, PartialEq)]
pub struct Signer {
    pubkey: PublicKey,
    private_key: SecretKey,
}

impl Debug for Signer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.pubkey)
    }
}

const CREDENTIAL_LEN: usize = digest::SHA256_OUTPUT_LEN;
const N_ITER: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(100_000) }; //todo tune len

impl Signer {
    pub fn from_file<T>(path: T, password: SecStr) -> Result<Self, Error>
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
        let pubkey = PublicKey::from_slice(pem_map.get("pubkey").expect("No pubkey in map"))?;
        let encrypted_private_key = pem_map
            .get("encrypted_private_key")
            .expect("No encrypted_keypair in pem file")
            .clone();
        let sym_key = Self::key_from_password(password, &*salt);
        let private_key =
            Self::private_key_from_encrypted(&*encrypted_private_key, &sym_key, &nonce);
        Ok(Self {
            pubkey,
            private_key,
        })
    }

    fn key_from_password(password: SecStr, salt: &[u8]) -> Key {
        let mut pbkdf2_hash = SecVec::new(vec![0; CREDENTIAL_LEN]);
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            N_ITER,
            salt,
            password.unsecure(),
            &mut pbkdf2_hash.unsecure_mut(),
        );
        dbg!(&pbkdf2_hash.unsecure());
        secretbox::Key::from_slice(&pbkdf2_hash.unsecure()).expect("Shouldn't panic")
    }

    fn private_key_from_encrypted(encrypted_key: &[u8], key: &Key, nonce: &Nonce) -> SecretKey {
        SecretKey::from_slice(
            &secretbox::open(encrypted_key, nonce, key).expect("Failed decrypting SecretKey"),
        )
        .expect("Failed constructing SecretKey from decrypted data")
    }

    pub fn sign(&self, data: &[u8]) -> Result<Signature, Error> {
        let mut eth_data: Vec<u8> = b"\x19Ethereum Signed Message:\n".to_vec();
        eth_data.extend_from_slice(data.len().to_string().as_bytes());
        eth_data.extend_from_slice(&data);
        let hash = Keccak256::digest(&eth_data);
        let message = Message::from_slice(&*hash)?;
        let secp = secp256k1::Secp256k1::new();
        Ok(secp.sign(&message, &self.private_key))
    }

    pub fn init<T>(pem_file_path: T, password: SecStr) -> Result<Self, Error>
    //todo use Writer instead of Path?
    where
        T: AsRef<Path>,
    {
        sodiumoxide::init().expect("Failed initializing libsodium");
        let nonce = secretbox::gen_nonce();
        let mut rng = rand::rngs::OsRng::new().expect("OsRng fail");
        let mut salt = vec![0u8; CREDENTIAL_LEN];
        rng.fill(salt.as_mut_slice());
        let key = Self::key_from_password(password, &salt);
        let curve = secp256k1::Secp256k1::new();
        let (private, public) = curve.generate_keypair(&mut rng);
        let encrypted_private_key = secretbox::seal(&private[..], &nonce, &key);
        let pubkey = Vec::from(public.serialize());
        let nonce_bytes = Vec::from(nonce.0);
        let pem_data = vec![
            Pem {
                tag: "pubkey".into(),
                contents: pubkey,
            },
            Pem {
                tag: "salt".into(),
                contents: salt,
            },
            Pem {
                tag: "encrypted_private_key".into(),
                contents: encrypted_private_key,
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
            pubkey: public,
            private_key: private,
        })
    }
}

#[cfg(test)]
mod test {
    use rand::Rng;
    use ring::pbkdf2;
    use secstr::SecStr;

    use crate::key_managment::{CREDENTIAL_LEN, Signer};

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
        let password = SecStr::new("123".into());
        let path = "./test/test_init.key";
        let signer = Signer::init(&path, password.clone()).unwrap();
        let read_signer = Signer::from_file(&path, password).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(read_signer, signer);
    }

    #[test]
    fn test_bad_password() {
        let password = SecStr::new("123".into());
        let path = "./test/test_bad.key";
        Signer::init(&path, password.clone()).unwrap();
        let result =
            std::panic::catch_unwind(|| Signer::from_file(&path, SecStr::new("lol".into())));
        std::fs::remove_file(path);
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
