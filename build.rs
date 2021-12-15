extern crate cc;

fn main() {
    cc::Build::new()
        .file("src/utils/protected_region/pkey.c")
        .compile("pkey-sys");
}
