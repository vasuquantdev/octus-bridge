# Launch in docker

## Build container

```bash
docker build -t relay .
```

## Run it

```bash
docker run relay -e config_string='json config'
```

## Config
All fields are like in [yaml config](CONFIGURATION.md)

```json
{
   "listen_address":"127.0.0.1:12345",
   "keys_path":"/var/lib/relay/keys.json",
   "storage_path":"/var/lib/relay/persistent_storage",
   "logger_settings":{
      "appenders":{
         "stdout":{
            "kind":"console",
            "encoder":{
               "pattern":"{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} {M} {f}:{L} = {m} {n}"
            }
         }
      },
      "root":{
         "level":"error",
         "appenders":[
            "stdout"
         ]
      },
      "loggers":{
         "relay":{
            "level":"info",
            "appenders":[
               "stdout"
            ],
            "additive":false
         },
         "relay_eth":{
            "level":"info",
            "appenders":[
               "stdout"
            ],
            "additive":false
         },
         "relay_ton":{
            "level":"info",
            "appenders":[
               "stdout"
            ],
            "additive":false
         }
      }
   },
   "metrics_settings":{
      "listen_address":"127.0.0.1:10000",
      "metrics_path":"/",
      "collection_interval":10
   },
   "eth_settings":{
      "node_address":"http://localhost:8545",
      "tcp_connection_count":100,
      "get_eth_data_timeout":10,
      "get_eth_data_attempts":50,
      "eth_poll_interval":10,
      "eth_poll_attempts":8640,
      "suspicious_blocks_offset":10,
      "bridge_address":"0x0000000000000000000000000000000000000000"
   },
   "ton_settings":{
      "relay_contract_address":"",
      "bridge_contract_address":"",
      "transport":{
         "type":"tonlib",
         "server_address":"54.158.97.195:3032",
         "server_key":"uNRRL+6enQjuiZ/s6Z+vO7yxUUR7uxdfzIy+RxkECrc=",
         "last_block_threshold":1,
         "subscription_polling_interval":1,
         "max_initial_rescan_gap":null,
         "max_rescan_gap":null
      },
      "event_configuration_details_retry_interval":5,
      "event_configuration_details_retry_count":100,
      "event_details_retry_interval":0,
      "event_details_retry_count":100,
      "message_retry_interval":60,
      "message_retry_count":10,
      "message_retry_interval_multiplier":1.5,
      "parallel_spawned_contracts_limit":10,
      "ton_events_verification_interval":1,
      "ton_events_verification_queue_lt_offset":10,
      "ton_events_allowed_time_diff":10,
      "events_handler_retry_count":50,
      "events_handler_interval":10
   }
}
```
