use anyhow::Error;
use ed25519_dalek::{Keypair, Signature};
use hex::encode;
use rand::rngs::OsRng;
use serde::export::fmt::Debug;
use serde::export::Formatter;
use serde_json::from_reader;
use sha3::{Digest, Keccak256};
use std::fs::{File, Permissions};
use std::path::Path;

pub struct Signer {
    keys: Keypair,
}

impl Debug for Signer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Pub:{:?}", self.keys.public)
    }
}

impl Signer {
    pub fn from_file<T>(path: T) -> Result<Self, Error>
    where
        T: AsRef<Path>,
    {
        let file = File::open(path)?;

        Ok(Self {
            keys: from_reader(&file)?,
        })
    }

    pub fn generate_key_pair() -> Self {
        Self {
            keys: Keypair::generate(&mut OsRng),
        }
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        use ed25519_dalek::Signer;
        let mut eth_data: Vec<u8> = b"\x19Ethereum Signed Message:\n".to_vec();
        eth_data.extend_from_slice(data.len().to_string().as_bytes());
        eth_data.extend_from_slice(&data);
        let hash = Keccak256::digest(&eth_data);
        self.keys.sign(&*hash)
    }

    pub fn dump_keys<T>(&self, path: T) -> Result<(), Error>
    where
        T: AsRef<Path>,
    {
        let  file = File::create(path)?;
        serde_json::to_writer(file, &self.keys)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::key_managment::Signer;
    use ed25519_dalek::ed25519::signature::Signature;
    use ed25519_dalek::Keypair;

    #[test]
    fn test_sign() {
        use hex::decode;
        let message_text = b"Some data";

        let signer = Signer::from_file("./test/test.keys").unwrap();
        let res = signer.sign(b"test");
        assert_eq!(res, Signature::from_bytes(decode("2d34bd34780a6fb76181c103653161c456eb7163eb9e098b29fc1619b46fa123ae3d962ca738e480a83ed1943e965b652175c78536781abe426f46cdfe64a209").unwrap().as_slice()).unwrap());
    }
}
