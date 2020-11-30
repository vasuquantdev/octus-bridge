FROM gcr.io/dexpa-175115/broxus/rust-runtime:1
COPY target/release/relay /app/application

ENTRYPOINT ["/app/entrypoint.sh"]