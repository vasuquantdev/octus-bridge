
FROM rust:1.51.0

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y libclang-dev cmake

# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=dialog
RUN rustup component add rustfmt
RUN mkdir relay
WORKDIR relay
RUN cargo install cargo-deb
COPY Cargo.lock ./Cargo.lock
COPY Cargo.toml ./Cargo.toml

# Main library
COPY src ./src
COPY debian debian
# supporting libraries
COPY relay-eth ./relay-eth
COPY relay-models ./relay-models
COPY relay-ton ./relay-ton
COPY client ./client
COPY relay-utils ./relay-utils
COPY LICENSE ./LICENSE
RUN ls -lah
RUN cargo deb
