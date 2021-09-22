<p align="center">
    <h3 align="center">relay</h3>
    <p align="center">ETH-TON bridge connecting link</p>
    <p align="center">
        <a href="/LICENSE">
            <img alt="GitHub" src="https://img.shields.io/github/license/broxus/ton-eth-bridge-relay" />
        </a>
    </p>
</p>

### Runtime requirements
- CPU: 8 cores, 2 GHz
- RAM: 16 GB
- Storage: 200 GB fast SSD
- Network: 100 MBit/s

### Docker build

The simplest way, but adds some overhead. Not recommended for machines with lower specs than required.

#### How to run

```bash
# Build docker container
docker build --tag relay .

# Run container
docker run -ti --rm \
  --mount type=bind,source=$(pwd)/state,target=/var/relay \
  --mount type=bind,source=$(pwd)/cfg,target=/cfg \
  -p 30303:30303/udp \
  relay
```

### Native build

A little bit more complex way, but gives some performance boost and reduces load.

#### Requirements
- Rust 1.54+
- Clang 11

#### How to run
```bash
export MASTER_PASSWORD=your_password
wget https://raw.githubusercontent.com/tonlabs/main.ton.dev/master/configs/main.ton.dev/ton-global.config.json
RUSTFLAGS='-C target-cpu=native' cargo run \
  --release -- \
  run --config config.yaml --global-config ton-global.config.json
```

### Example config

`config.yaml`

> NOTE: all parameters can be overwritten from environment

```yaml
---
# Keystore password
master_password: 12345678
# Your address from which you specified keys
staker_address: '0:a921453472366b7feeec15323a96b5dcf17197c88dc0d4578dfa52900b8a33cb'
bridge_settings:
  # Keystore data path
  keys_path: "/etc/relay/keys.json"
  # Bridge contract address
  bridge_address: "0:65d2002fae133c1064ae0f0ff44e416e52f112cf8faece53cd39e93d0f4d23d7"
  # If set, relay will not participate in elections. Default: false
  ignore_elections: false
  # EVM network configs
  networks:
    - chain_id: 5
      # RPC node endpoint
      endpoint: "https://goerli.infura.io/v3/your_key"
      # Timeout, used for simple getter requests
      get_timeout_sec: 10
      # Max simultaneous connection count
      pool_size: 10
      # Event logs polling interval
      poll_interval_sec: 10
      # Max total request duration
      maximum_failed_responses_time_sec: 600
  # ETH address verification settings (optional)
  address_verification:
    # Minimal balance on user's wallet to start address verification.
    # Default: 50000000
    min_balance_gwei: 50000000
    # Fixed gas price. Default: 300
    gas_price_gwei: 300
    # Path to the file with transaction state.
    # Default: "./verification-state.json"
    state_path: "verification-state.json"
node_settings:
  # UDP port, used for ADNL node. Default: 30303
  adnl_port: 30303
  # Root directory for relay DB. Default: "./db"
  db_path: "/var/relay/db"
  # Path to temporary ADNL keys. 
  # NOTE: Will be generated if it was not there.
  # Default: "./adnl-keys.json"
  temp_keys_path: "/var/relay/adnl-keys.json"
metrics_settings:
  # Listen address of metrics. Used by the client to gather prometheus metrics.
  # Default: "127.0.0.1:10000"
  listen_address: "127.0.0.1:10000"
  # Path to the metrics. Default: "/"
  metrics_path: "/"
  # Metrics update interval in seconds. Default: 10
  collection_interval_sec: 10
# log4rs settings.
# See https://docs.rs/log4rs/1.0.0/log4rs/ for more details
logger_settings:
  appenders:
    stdout:
      kind: console
      encoder:
        pattern: "{h({l})} {M} = {m} {n}"
  root:
    level: info
    appenders:
      - stdout
  loggers:
    ton_indexer:
      level: warn
      appenders:
        - stdout
      additive: false
    relay:
      level: info
      appenders:
        - stdout
      additive: false
    tiny_adnl:
      level: error
      appenders:
        - stdout
      additive: false
```
