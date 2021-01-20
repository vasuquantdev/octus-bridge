# ETH ↔️ TON relay

## Security

On first launch, user provides seed phrases for ton and ethereum and password,
with which them will be encrypted. We
use [xsalsa20poly1305](http://nacl.cr.yp.to/valid.html) for encryption
and [PBKDF2](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-132.pdf)
for user password derivation.

## Reliability

In case of panic, we flush all the state to disk. In normal conditions state is
periodically flushed on disk. In case of network error we retry to get/send
data. We are using http and polling instead of websockets due to impossibility
to detect the disconnect and save state for every processed block.

## How it works

There are two parts. TON → ETH and ETH → TON.

### ETH -> TON

Relay subscribes on address, specified in `bridge_contract_address`. Here it
obtains list of known config addresses. We subscribe on each config in this
list. Each config contract gives as configuration and stream of events in case
of config change.

Each config has ethereum address + abi + `blocks_to_confirm` constant + some
additional data.

Then we subscribe on ethereum events from the last block in ethereum, using abi
and addresses, got on the previous step. If the relay has ever started, then we
restore state in  [Persistent state](#persistent-state) section.

We enqueue each received ethereum event in the persistent queue. On each
processed block in ethereum we check, if any prepared events in queue are should
be broadcast(block number of enqueued event is equal or less than processed
block number +`blocks_to_confirm` and required number of votes for this event
has not been collected).

For each event received from other relays we check it validity.

### TON -> ETH

Subscribe on events in bridge contract. Get ton event contract. Get event proxy
address from it.  
Subscribe on event proxy. For each event got from proxy check event correctness,
pack it according to [conversion scheme](#supported-conversions). Sign it and
send to event contract.

You can read more about
contracts [here](https://github.com/broxus/ton-eth-bridge-contracts/tree/master/docs)
.

### Supported types in ethereum abi

- uint
- int
- address
- bool
- string
- bytes

Abi should be in json. Example:

```json
{
  "inputs": [
    {
      "name": "state",
      "type": "uint256"
    },
    {
      "name": "author",
      "type": "address"
    }
  ],
  "name": "StateChange"
}
```

All the fields are mandatory. Other fields will be ignored.

#### Supported conversions

#### Eth -> Ton

| Ethereum type | Ton type   |
| ------------- | ---------- |
| Address       | Bytes      |
| Bytes         | Bytes      |
| Int           | Int        |
| Bool          | Bool       |
| String        | Bytes      |
| Array         | Array      |
| FixedBytes    | FixedBytes |
| FixedArray    | FixedArray |
| Tuple         | Tuple      |

#### Ton -> Eth

| Ton type   | Ethereum type |
| ---------- | ------------- |
| FixedBytes | FixedBytes    |
| Bytes      | Bytes         |
| Uint       | Uint          |
| Int        | Int           |
| Bool       | Bool          |
| FixedArray | FixedArray    |
| Array      | Array         |
| Tuple      | Tuple         |

### Persistent state.

- We use embedded key value db for persistent storage and queuing.
- Every processed block in ethereum is written into db, to restore state after a
  shutdown.
- Every event from ethereum is enqueued into the db and moved to another queue
  when conditions are met.
- Every ton transaction is put to the persistent queue and move from it to
  confirmed table when we see confirmation for it.
- We resend all pending transaction after relay restart
- All transfers between queues are atomic.

## Configuration

You can generate default config,
using `relay --gen-config 'config.yaml'`

### Example config with graphql transport

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
eth_settings:
  node_address: "http://localhost:1234"
  tcp_connection_count: 100
ton_settings:
  bridge_contract_address: ""
  relay_contract_address: ""
  transport:
    type: graphql
    address: "https://main.ton.dev/graphql"
    next_block_timeout_sec: 60
    parallel_connections: 100
    fetch_timeout_secs: 10
  event_configuration_details_retry_interval: 5
  event_configuration_details_retry_count: 100
  event_details_retry_interval: 0
  event_details_retry_count: 100
  message_retry_interval: 60
  message_retry_count: 10
  message_retry_interval_multiplier: 1.5
  parallel_spawned_contracts_limit: 10
  ton_events_verification_interval: 1
``` 

- `keys_path` path to file, where encrypted data is stored.
- `storage_path` path for [database](#persistent-state)
- `listen_address` address to bind control server.  **EXPOSING IT TO OUTER WORLD
  IS PROHIBITED**, because anyone, having access to it can control relay.
- `number_of_ethereum_tcp_connections` maximum number of parallel tcp
  connections to ethereum node

#### eth_settings

- `node_address`  - address of ethereum node
- `tcp_connection_count` - maximum number of parallel tcp connections to
  ethereum node

#### ton_settings

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
- `ton_events_verification_interval`- interval for verification queue processing loop  

##### GraphQL

- `addr` - address of graphql endpoint
- `next_block_timeout_sec`  - timeout for blocks emission
- `parallel_connections` - amount of parallel connections to GraphQL
- `fetch_timeout_secs` - timeout for GraphQL queries

#### Tonlib

- `server_address` address of ton lite server
- `server_key` key of lite server
- `last_block_threshold_sec`  last block id caching duration
- `subscription_polling_interval_sec` how often accounts are polled. Has sense
  when it's greater or equal `last_block_threshold_sec`

## How to use

### First run

We provide prebuilt `.deb` packages, so you need to:

- run `sudo dpkg -i relay_0.1.0_amd64.deb`
- change [config](#configuration) in you favourite editor (default location is
  `/etc/relay.conf`)
- run it: `sudo systemctl start relay`
- init it: ` relay-client --server-addr ADDRESS_YOU_SET_AS_LISTEN_ADDRESS` and
  enjoy cli experience.

### Service restart

- run client and unlock the relay
