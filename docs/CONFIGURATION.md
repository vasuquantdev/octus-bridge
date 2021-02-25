# Configuration

You can generate default config, using `relay --gen-config 'config.yaml'`

## Example config with graphql transport

```yaml
---
listen_address: "127.0.0.1:12345"
keys_path: /var/lib/relay/keys.json
storage_path: /var/lib/relay/persistent_storage
logger_settings:
  appenders:
    stdout:
      kind: console
      encoder:
        pattern: "{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} {M} {f}:{L} = {m} {n}"
  root:
    level: error
    appenders:
      - stdout
  loggers:
    relay:
      level: info
      appenders:
        - stdout
      additive: false
    relay_eth:
      level: info
      appenders:
        - stdout
      additive: false
    relay_ton:
      level: info
      appenders:
        - stdout
      additive: false
metrics_settings:
  listen_address: "127.0.0.1:10000"
  metrics_path: "/"
  collection_interval: 10s
eth_settings:
  node_address: "http://localhost:8545"
  tcp_connection_count: 100
  get_eth_data_timeout: 10s
  get_eth_data_attempts: 50
  eth_poll_interval: 10s
  eth_poll_attempts: 8640
  suspicious_blocks_offset: 100
  bridge_address: ""
ton_settings:
  bridge_contract_address: ""
  relay_contract_address: ""
  transport:
    type: graphql
    address: "https://main.ton.dev/graphql"
    next_block_timeout: 60s
    parallel_connections: 100
    fetch_timeout: 10s
  event_configuration_details_retry_interval: 5s
  event_configuration_details_retry_count: 100
  event_details_retry_interval: 5s
  event_details_retry_count: 100
  message_retry_interval: 60s
  message_retry_count: 10
  message_retry_interval_multiplier: 1.5
  parallel_spawned_contracts_limit: 10
  ton_events_verification_interval: 1s
  ton_events_verification_queue_lt_offset: 10
  ton_events_allowed_time_diff: 10
  events_handler_retry_count: 50
  events_handler_interval: 10s
``` 

- `keys_path` path to file, where encrypted data is stored.
- `storage_path` path for database
- `listen_address` address to bind control server.  **EXPOSING IT TO OUTER WORLD
  IS PROHIBITED**, because anyone, having access to it can control relay.
- `number_of_ethereum_tcp_connections` maximum number of parallel tcp
  connections to ethereum node

### metrics_settings

- `listen_address` - address to bind metrics server
- `metrics_path` - the HTTP resource path on which to fetch metrics from
  targets.
- `collection_interval` - metrics poll interval. Requests between collections
  will return cached data

### eth_settings

- `node_address`  - address of ethereum node
- `tcp_connection_count` - maximum number of parallel tcp connections to
  ethereum node
- `get_eth_data_timeout` - timeout and delay between retries for getting
  non-critical data, like current eth sync status or current height
- `get_eth_data_attempts` - number of attempts for previous field
- `eth_poll_interval` - poll interval between fetching new blocks
- `eth_poll_attempts`  - number of attempts to get logs in the block
- `suspicious_blocks_offset` - offset in blocks for checking suspicious
  transactions
- `bridge_address` - address of bridge contract in ethereum

### ton_settings

- `bridge_contract_address` - address of bridge contract
- `relay_contract_address` - address of relay contract

  *Next section is optional to configure, default settings are just ok*

- `event_configuration_details_retry_interval` - time to wait between retries
- `event_configuration_details_retry_count` - times to get configuration details
  in case of errors
- `event_details_retry_interval` - time to wait between retries
- `event_details_retry_count` - times to get event details in case of errors
- `message_retry_interval` - interval between resending messages
- `message_retry_count`- number of retries to send message
- `message_retry_interval_multiplier`- coefficient, on which every interval will
  be multiplied
- `parallel_spawned_contracts_limit`- amount of parallel sent messages
- `ton_events_verification_interval`- interval for verification queue processing
  loop
- `ton_events_verification_queue_lt_offset` - lt delay before current logical
  time

#### GraphQL

- `address` - address of graphql endpoint
- `next_block_timeout`  - timeout for blocks emission
- `fetch_timeout` - timeout for GraphQL queries
- `parallel_connections` - amount of parallel connections to GraphQL

### Tonlib

- `server_address` address of ton lite server
- `server_key` key of lite server
- `last_block_threshold`  last block id caching duration
- `subscription_polling_interval` how often accounts are polled. Has sense when
  it's greater or equal `last_block_threshold_sec`


## How to use

### First run

We don't provide prebuilt `.deb` packages due security reasons. So, to
get `.deb`
you should run this:

- `./scripts/build_deb.sh`
- run `sudo dpkg -i name_of_deb_package_you_got_on_previous_step`
- change config in you favourite editor (default location is
  `/etc/relay.conf`)
- run it: `sudo systemctl start relay`
- init it: ` relay-client --server-addr ADDRESS_YOU_SET_AS_LISTEN_ADDRESS` and
  enjoy cli experience.

### Service restart

- run client and unlock the relay

### Db operations

If you want to fully reinitialize relay for some reason, remove all files
in `keys_path` and in `storage_path`