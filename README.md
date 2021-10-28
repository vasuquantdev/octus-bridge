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

### How to run

To simplify the build and create some semblance of standardization in this repository 
there is a set of scripts for configuring the relay.

NOTE: scripts are prepared and tested on **Ubuntu 20.04**. You may need to modify them a little for other distros.

1. ##### Setup relay service
   * Native version (a little more complex way, but gives some performance gain and reduces the load):
     ```bash
     ./scripts/setup.sh -t native
     ```
   * **OR** docker version (the simplest way, but adds some overhead):
     ```bash
     ./scripts/setup.sh -t docker
     ```
     Not recommended for machines with lower specs than required

   > At this stage, a systemd service `relay` is created. Configs and keys will be in `/etc/relay` and
   > TON node DB will be in `/var/db/relay`. 

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
   Use ETH address and TON public key from the previous step to link this relay setup
   with your staker address at https://v2.tonbridge.io/relayers/create

   > During linking you will need to send at least **0.05 ETH** to your relay ETH address so that the relay can confirm his ownership.
   > 
   > If you think that this is a lot or if the gas price is more than 300 gwei now, 
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

   > Relay has a built-in Prometheus metrics exporter which is configured in the `metrics_settings` section of the config.
   > By default, metrics are available at `http://127.0.0.1:10000/`
   > 
   > <details><summary><b>Response example:</b></summary>
   > <p>
   >
   > ```
   > eth_subscriber_last_processed_block{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="250"} 19757952
   > eth_subscriber_pending_confirmation_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="250"} 0
   > eth_subscriber_last_processed_block{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="1"} 13467334
   > eth_subscriber_pending_confirmation_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="1"} 0
   > eth_subscriber_last_processed_block{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="137"} 20486774
   > eth_subscriber_pending_confirmation_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="137"} 0
   > eth_subscriber_last_processed_block{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="56"} 12140031
   > eth_subscriber_pending_confirmation_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",chain_id="56"} 0
   > ton_subscriber_ready{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} true
   > ton_subscriber_current_utime{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 1635368353
   > ton_subscriber_time_diff{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 5
   > ton_subscriber_pending_message_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 0
   > bridge_pending_eth_event_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 0
   > bridge_pending_ton_event_count{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 0
   > staking_user_data_tokens_balance{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 100000000000000
   > staking_current_relay_round{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246"} 5
   > staking_elections_start_time{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",round_num="5"} 1635541843
   > staking_elections_status{staker="0:7a9701bede7f86bf039aba200c1bb421a388bbb4b0580bfaeafa66f908d2b246",round_num="5"} 0
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
      # Maximum blocks range for getLogs request
      max_block_range: 5000
    # Smart Chain
    - chain_id: 56
      # Public endpoint
      endpoint: https://bsc-dataseed.binance.org
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 10
      max_block_range: 5000
    # Fantom Opera
    - chain_id: 250
      # Public endpoint
      endpoint: https://rpc.ftm.tools
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 10
      max_block_range: 5000
    # Polygon
    - chain_id: 137
      # Public endpoint
      endpoint: "${POLYGON_URL}"
      get_timeout_sec: 10
      pool_size: 10
      poll_interval_sec: 10
      max_block_range: 5000
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
    relay:
      level: info
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
to the one "light" node which was constantly restarted or to the graphql which also was quite unreliable.
As a result, relays instantly see all events and vote for them almost simultaneously in one block. The implementation is
more optimized than C++ node, so they don't harm the network.
