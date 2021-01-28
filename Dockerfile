
FROM rust:1.49.0 as builder

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y libclang-dev cmake

# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=dialog
RUN mkdir relay
WORKDIR relay
COPY Cargo.lock ./Cargo.lock
COPY Cargo.toml ./Cargo.toml

# Main library
COPY src ./src
# supporting libraries
COPY relay-eth ./relay-eth
COPY relay-models ./relay-models
COPY relay-ton ./relay-ton
COPY client ./client
COPY relay-utils ./relay-utils
COPY LICENSE ./LICENSE
RUN ls -lah
RUN cargo build --release

FROM debian:buster-slim

RUN useradd -ms /bin/bash relay

RUN apt-get update &&  apt-get install -y --no-install-recommends   libsnappy1v5 gnupg2 openssl ca-certificates wget librdkafka1 libsasl2-2 gettext-base libpq-dev  default-libmysqlclient-dev \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder relay/target/release/relay /relay
# Entrypoint
COPY scripts/entrypoint.sh /entrypoint.sh
RUN mkdir cfg


ENTRYPOINT ["/entrypoint.sh"]
