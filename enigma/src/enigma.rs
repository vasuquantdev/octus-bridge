use secstr::SecBox;
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::{Key, Nonce};
use std::ops::Deref;
use std::sync::Arc;
use std::marker::PhantomData;

struct EncryptedBox<T>
{
    salt: Vec<u8>,
    nonce: Nonce,
    symmetric_key: Key,
    data: Vec<u8>,
    _a: PhantomData<T>
}

impl <'a, T>Deref for EncryptedBox<T>
    where
        T: AsRef<&'a [u8]> + From<&'a [u8]>,
{
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        match secretbox::open(&self.data, &self.nonce, &self.symmetric_key).ok(){
            Some(a) => {
                &Some(T::from(&a))
            },
            _ => &None
        }
    }
}

impl EncryptedBox<T>
{
    // pub fn encrypt<T>(self: Arc<Enigma>, data: T) -> Vec<u8>
    // where
    //     T: AsRef<[u8]>,
    // {
    //     secretbox::seal(&data.as_ref(), &self.nonce, &self.symmetric_key)
    // }

    // pub fn decrypt<T>(&self) -> Result<Vec<u8>, ()>
    // {
    // 
    // }

    pub fn new<'a, T>(salt: Vec<u8>, nonce: Nonce, symmetric_key: Key, data: T) -> Self
        where
            T: AsRef<&'a [u8]> + From<&'a [u8]>,
    {
        EncryptedBox {
            nonce,
            salt,
            symmetric_key,
             data: Vec::from(*data.as_ref()),
            _a: Default::default()
        }
    }
}
