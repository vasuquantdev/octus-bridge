FROM rust:1.56.0-bullseye as builder

ENV DEBIAN_FRONTEND=noninteractive
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | \
    gpg --dearmor | \
    tee /usr/share/keyrings/llvm-toolchain.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/llvm-toolchain.gpg] http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-13 main" >> /etc/apt/sources.list \
    echo "deb-src http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-13 main" >> /etc/apt/sources.list
RUN apt-get update && \
    apt-get install -y pkg-config openssl clang-13

RUN mkdir relay
WORKDIR relay
COPY Cargo.lock ./Cargo.lock
COPY Cargo.toml ./Cargo.toml
COPY src ./src
COPY LICENSE ./LICENSE
RUN ls -lah

RUN rustup component add rustfmt
RUN cargo build --release


FROM debian:bullseye-slim as runtime

RUN useradd -ms /bin/bash relay
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      openssl \
      ca-certificates \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /etc/bridge
COPY --from=builder relay/target/release/relay /usr/local/bin/relay

ENTRYPOINT ["relay"]
