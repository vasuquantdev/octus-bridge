FROM debian:stretch-slim

WORKDIR /app

RUN mkdir -p /usr/local/cargo/bin/ \
    && apt-get update \
    && apt-get install -y wget gnupg2 openssl ca-certificates \
    && apt-get purge -y wget \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir bin \
    && adduser --no-create-home runuser \
    && chown runuser:runuser -R /app


COPY target/release/relay /app/application
COPY entrypoint.sh /app/entrypoint.sh

USER runuser

ENV PATH=$PATH:/usr/local/cargo/bin/

EXPOSE 9000

ENTRYPOINT ["/app/entrypoint.sh"]

