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

# Prepare relay config
mkdir cfg
# Fill it using the example below
vim cfg/config.yaml

# Download network config
wget -O cfg/ton-global.config.json \
  https://raw.githubusercontent.com/tonlabs/main.ton.dev/master/configs/main.ton.dev/ton-global.config.json

# Run container
# (you may also need to pass some environment variables specified in the config)
docker run -ti --rm \
  --mount type=bind,source=$(pwd)/state,target=/var/relay \
  --mount type=bind,source=$(pwd)/cfg,target=/cfg \
  -p 30303:30303/udp \
  relay
```

### Native build

A little more complex way, but gives some performance gain and reduces the load.

#### Requirements
- Rust 1.54+
- Clang 11
- openssl-dev

#### How to run
```bash
RUSTFLAGS='-C target-cpu=native' cargo build --release

# Prepare relay config
mkdir cfg
# Fill it using the example below
vim ./cfg/config.yaml

# Download network config
wget -O ./cfg/ton-global.config.json \
  https://raw.githubusercontent.com/tonlabs/main.ton.dev/master/configs/main.ton.dev/ton-global.config.json

export MASTER_PASSWORD=your_password
target/release/relay run \
  --config cfg/config.yaml \
  --global-config cfg/ton-global.config.json
```

> NOTE: If you want to make a systemd service for the relay, then it is better to set it `Restart=no`
> 
> Example:
> ```
> [Unit]
> Description=relay
> After=network.target
> StartLimitIntervalSec=0
>
> [Service]
> Type=simple
> Restart=no
> WorkingDirectory=/etc/bridge
> ExecStart=/usr/local/bin/relay run --config /etc/bridge/config.yaml --global-config /etc/bridge/ton-global.config.json
> Environment=MASTER_PASSWORD=superpassword
> Environment=RELAY_STAKER_ADDRESS=0:1111111111111111111111111111111111111111111111111111111111111111
>
> [Install]
> WantedBy=multi-user.target
> ```

### Example config

`config.yaml`

> NOTE: The syntax `${VAR}` can also be used everywhere in config. It will be
> replaced by the value of the environment variable `VAR`.

```yaml
---
# Keystore password
master_password: "${MASTER_PASSWORD}"
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
    # Ethereum
    - chain_id: 1
      # RPC node endpoint
      endpoint: "${ETH_MAINNET_URL}"
      # Timeout, used for simple getter requests
      get_timeout_sec: 10
      # Max simultaneous connection count
      pool_size: 10
      # Event logs polling interval
      poll_interval_sec: 10
    # Smart Chain
    - chain_id: 56
      endpoint: https://bsc-dataseed.binance.org
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 10
      max_block_range: 5000
    # Fantom Opera
    - chain_id: 250
      endpoint: https://rpc.ftm.tools
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 10
    # Polygon
    - chain_id: 137
      endpoint: https://matic-mainnet.chainstacklabs.com
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 10
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
  # URL path to the metrics. Default: "/"
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

### Architecture overview

The relay is simultaneously the TON node and can communicate with all EVM networks specified in the config. 
Its purpose is to check and sign transaction events.

At startup, it synchronizes the TON node and downloads blockchain state. It searches Bridge contract state in it,
all connectors, active configurations and pending events. Then it subscribes to the Bridge contract in TON and 
listens for connector deployment events. Each new connector can produce activation event which signals that relay
should subscribe to the event configuration contract. Each configuration contract produces event deployment 
events, relay sees and checks them.

- For TON-to-EVM events, only the correctness of data packing is checked (all other stuff is verified on the contracts 
  side). If the event is correct, the relay converts this data into ETH ABI encoded bytes and signs it with its
  ETH key. This signature is sent along with a confirmation message. If the data in the event was invalid then the 
  relay sends a rejection message.
  
  ##### TON-to-EVM ABI mapping rules:
  ```
  bytes => same
  string => same
  uintX => same
  intX => same
  bool => same
  fixedbytes => same
  fixedarray => same but mapped
  array => same but mapped
  tuple => same but mapped
  _ => unsupported
  ```

- For EVM-to-TON events, all event parameters are checked on the relay side. It waits for or looking for a transaction
  on the specified EVM network, converts event data to the TVM cell and sends a confirmation message if everything was
  correct. Otherwise, it sends a rejection message.

  ##### ETH-to-TON ABI mapping rules:
  ```
  address => bytes (of length 20)
  bytes => bytes or cell (*)
  string => same
  intX => same
  uintX => same
  bool => same
  array => same but mapped
  fixedbytes1 => depends on context (**)
  fixedbytesX => same
  fixedarray => same but mapped
  tuple => same but mapped or cell (***)
  _ => unsupported
  ```

  > When converting ABI from EVM format to TON, there is a mechanism for controlling this process.
  > You can add a `bytes1` *(\*\*)* element which sets context flags to its value.
  > 
  > Currently, there are only three flags:
  > - `0x01` - place tuples to new cell (***)
  > - `0x02` - interpret `bytes` as encoded TVM cell (*)
  > - `0x04` - insert default cell in case of error with flag `0x02` (*)
  > 
  > NOTE: Flags can't be changed inside an array element! This would lead to inconsistent array items ABI.

The decision to make the relay a TON node was not made by chance. In the first version, several relays were connected 
to the one "light" (4TB goes brr) node which was constantly restarted or to the graphql which also was quite unreliable.
As a result, relays instantly see all events and vote for them almost simultaneously in one block. The implementation is
more optimized than C++ node, so they don't harm the network.
