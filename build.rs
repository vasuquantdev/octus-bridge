extern crate cc;

fn main() {
    cc::Build::new()
        .file("src/engine/keystore/pkey.c")
        .compile("pkey-sys");
}
