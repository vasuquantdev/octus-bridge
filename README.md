<p align="center">
    <h3 align="center">Octus Bridge relay</h3>
    <p align="center">
        <a href="/LICENSE">
            <img alt="GitHub" src="https://img.shields.io/github/license/broxus/octusbridge-relay" />
        </a>
    </p>
</p>

### Runtime requirements

- CPU: 8 cores, 2 GHz
- RAM: 16 GB
- Storage: 200 GB fast SSD
- Network: 100 MBit/s

### How to run

To simplify the build and create some semblance of standardization in this repository
there is a set of scripts for configuring the relay.

NOTE: scripts are prepared and tested on **Ubuntu 20.04**. You may need to modify them a little for other distros.

1. ##### Setup relay service

   - Native version (a little more complex way, but gives some performance gain and reduces the load):
     ```bash
     ./scripts/setup.sh -t native
     ```
   - **OR** docker version (the simplest way, but adds some overhead):
     ```bash
     ./scripts/setup.sh -t docker
     ```
     Not recommended for machines with lower specs than required

   > At this stage, a systemd service `relay` is created. Configs and keys will be in `/etc/relay` and
   > Everscale node DB will be in `/var/db/relay`.

   **Do not start this service yet!**

2. ##### Prepare config

   Either add the environment variables to the `[Service]` section of unit file.
   It is located at `/etc/systemd/system/relay.service`.

   > This method is recommended because you can just make a backup of `/etc/relay` and
   > not be afraid for it's safety, all keys in it are encrypted

   ```unit file (systemd)
   [Service]
   ...
   Environment=RELAY_MASTER_KEY=stub-master-key
   Environment=RELAY_STAKER_ADDRESS=your-staker-address
   Environment=ETH_MAINNET_URL=eth-http-rpc-endpoint
   Environment=POLYGON_URL=polygon-http-rpc-endpoint
   ...
   ```

   Or simply replace the `${..}` parameters in the config. It is located at `/etc/relay/config.yaml`.

3. ##### Generate keys

   ```bash
     ./scripts/generate.sh -t docker # or `native`, depends on your installation type
   ```

   > if you already have seed phrases that you want to import, then add the `-i` flag

   This script will print unencrypted data. **Be sure to write it down somewhere! And also make a backup of the `/etc/relay` folder!**

4. ##### Link relay keys

   Use ETH address and Everscale public key from the previous step to link this relay setup
   with your staker address at https://octusbridge.io/relayers/create. When you start the linking process go to step 5 and start the relay. It will begin to confirm the public key and address on the air.
   It may take some time to sync at first (~40 minutes).

   > During linking you will need to send at least **0.05 ETH** to your relay ETH address so that the relay can confirm his ownership.
   >
   > If you think that this is a lot or if the gas price is more than 300 GWEI now,
   > then you can change the values in the config.

5. ##### Enable and start relay service

   ```bash
   systemctl enable relay
   systemctl start relay

   # Optionally check if it is running normally. It will take some time to start.
   # Relay is fully operational when it prints `Initialized relay`
   journalctl -fu relay
   ```

   Relay will be running on UDP port `30000` by default, so make sure that this port is not blocked by firewall.

   NOTE: docker installation uses port 30000 in unit file, so you may need to update the service if you decide to change it.
   All environment variables must also be passed to container (e.g. `-e RELAY_MASTER_KEY`).

   > Relay has a built-in Prometheus metrics exporter which is configured in the `metrics_settings` section of the config.
   > By default, metrics are available at `http://127.0.0.1:10000/`
   >
   > <details><summary><b>Response example:</b></summary>
   > <p>
   >
   > ```
   > eth_subscriber_last_processed_block{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="56"} 13790361
   > eth_subscriber_pending_confirmation_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="56"} 0
   > eth_subscriber_last_processed_block{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="137"} 22954791
   > eth_subscriber_pending_confirmation_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="137"} 0
   > eth_subscriber_last_processed_block{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="250"} 26020394
   > eth_subscriber_pending_confirmation_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="250"} 0
   > eth_subscriber_last_processed_block{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="1"} 13875962
   > eth_subscriber_pending_confirmation_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="1"} 0
   > sol_subscriber_unrecognized_proposals_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 0
   > ton_subscriber_ready{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 1
   > ton_subscriber_current_utime{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 1640456699
   > ton_subscriber_time_diff{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 3
   > ton_subscriber_shard_client_time_diff{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 7
   > ton_subscriber_mc_block_seqno{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 13426600
   > ton_subscriber_shard_client_mc_block_seqno{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 13426600
   > ton_subscriber_pending_message_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 0
   > bridge_pending_eth_ton_event_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 0
   > bridge_pending_ton_eth_event_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 0
   > bridge_pending_sol_ton_event_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 0
   > bridge_pending_ton_sol_event_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 0
   > bridge_total_active_eth_ton_event_configurations{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 86
   > bridge_total_active_ton_eth_event_configurations{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 11
   > bridge_total_active_sol_ton_event_configurations{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 1
   > bridge_total_active_ton_sol_event_configurations{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 1
   > staking_user_data_tokens_balance{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",round_num="13"} 100000000000000
   > staking_current_relay_round{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 13
   > staking_elections_start_time{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",round_num="13"} 1640380268
   > staking_elections_end_time{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",round_num="13"} 1640553068
   > staking_elections_status{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",round_num="13"} 1
   > staking_ignore_elections{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",round_num="13"} 0
   > staking_participates_in_round{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",round_num="13"} 1
   > staking_elected{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",round_num="13"} 1
   > ```
   >
   > </p>
   > </details>

### Example config

> NOTE: The syntax `${VAR}` can also be used everywhere in config. It will be
> replaced by the value of the environment variable `VAR`.

```yaml
---
# Keystore password
master_password: "${RELAY_MASTER_KEY}"
# Your address from which you specified keys
staker_address: "${RELAY_STAKER_ADDRESS}"
bridge_settings:
  # Keystore data path
  keys_path: "/etc/relay/keys.json"
  # Bridge contract address
  bridge_address: "0:1d51fb47566d0d283ebbf83c641c01ebebaad6c3cec55895b0074b802036094e"
  # If set, relay will not participate in elections. Default: false
  ignore_elections: false
  # Solana network config
  sol_network:
    # Public endpoint
    endpoint: https://solana-api.projectserum.com
    # Commitment level
    commitment: "finalized"
  # EVM network configs
  evm_networks:
    # Ethereum
    - chain_id: 1
      # RPC node HTTP endpoint
      endpoint: "${ETH_MAINNET_URL}"
      # Timeout, used for simple getter requests. Default: 10
      get_timeout_sec: 10
      # Timeout, used for processing eth_getLogs response. Default: 120
      blocks_processing_timeout_sec: 120
      # Max simultaneous connection count. Default: 10
      pool_size: 10
      # Idle polling interval. Default: 60
      poll_interval_sec: 60
      # Maximum blocks range for getLogs request
      max_block_range: 5000
    # Smart Chain
    - chain_id: 56
      # Public endpoint (see https://docs.bscscan.com/misc-tools-and-utilities/public-rpc-nodes)
      endpoint: https://bsc-dataseed1.binance.org
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 60
      max_block_range: 5000
    # Fantom Opera
    - chain_id: 250
      # Public endpoint
      endpoint: https://rpc.ftm.tools
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 60
      max_block_range: 5000
    # Polygon
    - chain_id: 137
      endpoint: "${POLYGON_URL}"
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 60
      max_block_range: 5000
    # Milkomeda
    - chain_id: 2001
      endpoint: https://rpc-mainnet-cardano-evm.c1.milkomeda.com
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 60
      maximum_failed_responses_time_sec: 604800
    # Avalanche Network
    - chain_id: 43114
      endpoint: https://api.avax.network/ext/bc/C/rpc
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 60
      maximum_failed_responses_time_sec: 604800
node_settings:
  # Root directory for relay DB. Default: "./db"
  db_path: "/var/db/relay"
  # UDP port, used for ADNL node. Default: 30303
  adnl_port: 30000
  # Path to temporary ADNL keys.
  # NOTE: Will be generated if it was not there.
  # Default: "./adnl-keys.json"
  temp_keys_path: "/etc/relay/adnl-keys.json"
metrics_settings:
  # Listen address of metrics. Used by the client to gather prometheus metrics.
  # Default: "127.0.0.1:10000"
  listen_address: "127.0.0.1:10000"
  # URL path to the metrics. Default: "/"
  # Example: `curl http://127.0.0.1:10000/`
  metrics_path: "/metrics"
  # Metrics update interval in seconds. Default: 10
  collection_interval_sec: 10
```

### Architecture overview

The relay is simultaneously the Everscale node and can communicate with all EVM networks specified in the config.
Its purpose is to check and sign transaction events.

At startup, it synchronizes the Everscale node and downloads blockchain state. It searches Bridge contract state in it,
all connectors, active configurations and pending events. Then it subscribes to the Bridge contract in Everscale and
listens for connector deployment events. Each new connector can produce activation event which signals that relay
should subscribe to the event configuration contract. Each configuration contract produces event deployment
events, relay sees and checks them.

- For Everscale-to-EVM events, only the correctness of data packing is checked (all other stuff is verified on the contracts
  side). If the event is correct, the relay converts this data into ETH ABI encoded bytes and signs it with its
  ETH key. This signature is sent along with a confirmation message. If the data in the event was invalid then the
  relay sends a rejection message.

  ##### Everscale-to-EVM ABI mapping rules:

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

- For EVM-to-Everscale events, all event parameters are checked on the relay side. It waits for or looking for a transaction
  on the specified EVM network, converts event data to the TVM cell and sends a confirmation message if everything was
  correct. Otherwise, it sends a rejection message.

  ##### ETH-to-Everscale ABI mapping rules:

  > You can use https://github.com/broxus/eth-ton-abi-converter to convert data from web page

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

  > When converting ABI from EVM format to Everscale, there is a mechanism for controlling this process.
  > You can add a `bytes1` _(\*\*)_ element which sets context flags to its value.
  >
  > Currently, there are only three flags:
  >
  > - `0x01` - place tuples to new cell (\*\*\*)
  > - `0x02` - interpret `bytes` as encoded TVM cell (\*)
  > - `0x04` - insert default cell in case of error with flag `0x02` (\*)
  >
  > NOTE: Flags can't be changed inside an array element! This would lead to inconsistent array items ABI.

The decision to make the relay an Everscale node was not made by chance. In the first version, several relays were connected
to the one "light" node which was constantly restarted or to the graphql which also was quite unreliable.
As a result, relays instantly see all events and vote for them almost simultaneously in one block. The implementation is
more optimized than C++ node, so they don't harm the network.

### Changelog

### 2.2.0 (2023-04-04)

Bugfixes:

- Stability fixes

### 2.1.2 (2022-12-23)

Features:

- Improved logger
- Added support for configuration feature flags
- Extend rejection info

Bugfixes:

- Fixed transport issues

### 2.1.1 (2022-10-14)

Bugfixes:

- Fixed errors with SOL events during scanning all events
- Increased default polling interval

### 2.1.0 (2022-09-06)

Features:

- Add Solana

Bugfixes:

- Fixed outgoing RLDP transfers

### 2.0.13 (2022-07-22)

Features:

- Replace `tiny-adnl` with `everscale-network`
- Optimize DB layout

### 2.0.12 (2022-06-03)

Features:

- Backport transport fixes

### 2.0.11 (2022-04-13)

Features:

- Added archives assembly
- Added new account model support

Bugfixes

- Fixed ADNL channels

### 2.0.10 (2022-04-03)

Bugfixes

- Fixed memory leaks. (New peers queue was read at a fixed rate).

### 2.0.9 (2022-03-26)

Features

- Added packets compression support (enabled by default).
- Various optimizations.

### 2.0.8 (2022-02-12)

Features

- Updated ABI version to 2.2
- Fixed memory leaks. (Shard states were slowly filling with loaded storage cells).

### 2.0.7 (2022-02-02)

Features

- ADNL security improvements

### 2.0.6 (2021-12-31)

Features

- Optimized DB structure

### 2.0.5 (2021-11-27)

Features

- Improved ETH events verification
- Updated events ABI
- Added `ton_subscriber_shard_client_time_diff`, `ton_subscriber_mc_block_seqno` and `ton_subscriber_shard_client_mc_block_seqno`
  values into metrics

### 2.0.4 (2021-11-11)

Features

- Use jemalloc by default

Bugfixes

- Fixed blocks GC memory issues

### 2.0.3 (2021-11-09)

Features

- Hot reload for metrics exporter and logger settings (SIGHUP signal)
- Blocks and states garbage collection
- Additional EVM RPC timing controls
- Improved database layout and increased data locality

Bugfixes

- Fixed exported metrics format
- Fixed event confirmation counters
- Fixed time diff metrics
- Reduced blocks range for `eth_getLogs`
- Ignore descending blocks for ETH rpc

### 2.0.2

Features

- Added setup scripts for fast deployment

Bugfixes

- Fixed bridge contracts interaction logic according to new changes

### 2.0.1

Initial release
