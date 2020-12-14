pub mod key_managment;
pub mod recovery;

///We are using `libsodium` for encryption.
/// It provides better security, than aes in our case.
/// Password recovery isn't possible.
/// For the password deriving we use pbkdf2 function with a huge amount of rounds.
