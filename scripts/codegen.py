import os

compose_template = """
version: "3"
services:
"""
relay_str = """
        relay{}:
            build: ..
            network_mode: host
            volumes:
              - type: bind
                source: "{}"
                target: "/cfg"
"""

config_template = """
{
  "encrypted_data": "/cfg/config_data.json",
  "eth_node_address": "http://3.239.148.226:8545",
  "ton_contract_address": "0:0a2cfeb23aec1822e48cb7d0a402d24c4e8dddbc42c125df99b6aba2bec372b7",
  "ton_derivation_path": "m/44'/396'/0'/0/{}",
  "storage_path": "/cfg/persistent_storage",
  "listen_address": "127.0.0.1:{}",
  "ton_config": {
    "type": "GraphQL",
    "addr": "http://3.239.148.226/graphql",
    "next_block_timeout_sec": 60
  },
  "ton_operation_timeouts": {
    "configuration_contract_try_poll_times": 100,
    "get_event_details_retry_times": 100,
    "get_event_details_poll_interval_secs": 5,
    "broadcast_in_ton_interval_secs": 10,
    "broadcast_in_ton_times": 50,
    "broadcast_in_ton_interval_multiplier": 1.5
  }
}
"""

for i in range(0, 13):
    port = 10000
    config_path = "./relay{}".format(i)
    compose_template += relay_str.format(i, config_path)
    os.mkdir(config_path)
    config_data = config_template.replace("{}", str(i + port))
    config = open(config_path + "/config.json", "w")
    config.write(config_data)

with open("docker-compose.yaml", "w") as f:
    f.write(compose_template)
