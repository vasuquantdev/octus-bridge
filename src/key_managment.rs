use anyhow::Error;
use ed25519_dalek::{Keypair,  Signature};
use rand::rngs::OsRng;
use serde::export::fmt::Debug;
use serde::export::Formatter;
use serde_json::from_reader;
use std::fs::{File, Permissions};
use std::path::Path;
use sha3::{Keccak256, Digest};
use hex::encode;

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

    pub fn sign(&self, data: &[u8]) -> Signature
    {
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
        let mut file  = File::create(path)?;
        serde_json::to_writer(file, &self.keys)?;
        Ok(())
    }
}

#[cfg(test)]
mod test{
    use crate::key_managment::Signer;
    #[test]
    fn test_sign(){
        let signer = Signer::from_file("./test/test.keys").unwrap();
        println!("{}", hex::encode(signer.keys.public.as_bytes()));
        dbg!(signer);
    }
}