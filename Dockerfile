FROM rust:1.54.0 as builder

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive
RUN echo "deb http://apt.llvm.org/buster/ llvm-toolchain-buster-11 main" >> /etc/apt/sources.list \
    echo "deb-src http://apt.llvm.org/buster/ llvm-toolchain-buster-11 main" >> /etc/apt/sources.list
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN apt-get update \
    && apt-get install -y clang-11 cmake

RUN mkdir relay
WORKDIR relay
COPY Cargo.lock ./Cargo.lock
COPY Cargo.toml ./Cargo.toml
COPY src ./src
COPY LICENSE ./LICENSE
RUN ls -lah

RUN rustup component add rustfmt
RUN cargo build --release


FROM debian:buster-slim as runtime

RUN useradd -ms /bin/bash relay

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      openssl \
      ca-certificates \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder relay/target/release/relay /relay

# Entrypoint
COPY scripts/entrypoint.sh /entrypoint.sh
RUN mkdir cfg


ENTRYPOINT ["/entrypoint.sh"]
