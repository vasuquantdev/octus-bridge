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
listen_address: ""127.0.0.1:{}"
keys_path: "/cfg/config_data.json"
storage_path: "/cfg/persistent_storage"
logger_settings:
  appenders:
    stdout:
      kind: console
      encoder:
        pattern: "{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} {M} {f}:{L} = {m} {n}"
    all:
      kind: file
      path: "log/requests.log"
      encoder:
        pattern: "{d} - {M} {f}:{L} = {m}{n}"
  root:
    level: error
    appenders:
      - stdout
  loggers:
    relay:
      level: debug
      appenders:
        - all
        - stdout
      additive: false
    relay_eth:
      level: debug
      appenders:
        - all
        - stdout
      additive: false
    relay_ton:
      level: debug
      appenders:
        - all
        - stdout
      additive: false
eth_settings:
  node_address: "ws://3.239.148.226:8545"
  tcp_connection_count: 100
ton_settings:
  bridge_contract_address: '0:42b18b796069f4ae645bf58f073df820f76d2eccaff535ec2f2d53f097e5f9fe'
  seed_derivation_path  : "m/44'/396'/0'/0/5"
  transport:
    type: graphql
    address: "http://3.239.148.226/graphql"
    next_block_timeout_sec: 60
  event_configuration_details_retry_interval: 5
  event_configuration_details_retry_count: 100
  event_details_retry_interval: 0
  event_details_retry_count: 100
  message_retry_interval: 60
  message_retry_count: 10
  message_retry_interval_multiplier: 1.5
"""

for i in range(0, 13):
    port = 10000
    config_path = "./relay{}".format(i)
    compose_template += relay_str.format(i, config_path)
    os.mkdir(config_path)
    config_data = config_template.replace("{}", str(i + port))
    config = open(config_path + "/config.yaml", "w")
    config.write(config_data)

with open("docker-compose.yaml", "w") as f:
    f.write(compose_template)
