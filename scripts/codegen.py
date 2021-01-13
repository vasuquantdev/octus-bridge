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
listen_address: "127.0.0.1:{port}"
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
  tcp_connection_count: 1
ton_settings:
  bridge_contract_address: '0:07d898301a542a686ea86b7137d063b5886f6614187ab37754f63a0d4b2d808a'
  seed_derivation_path  : "m/44'/396'/0'/0/{der_path}"
  transport:
    type: graphql
    address: "http://3.239.148.226/graphql"
    next_block_timeout_sec: 60
    parallel_connections: 1
  event_configuration_details_retry_interval: 5
  event_configuration_details_retry_count: 100
  event_details_retry_interval: 0
  event_details_retry_count: 100
  message_retry_interval: 20
  message_retry_count: 50
  message_retry_interval_multiplier: 1.1
"""

for i in range(0, 3):
    port = 10000 + i
    config_path = "./relay{}".format(i)
    compose_template += relay_str.format(i, config_path)
    os.mkdir(config_path)
    config_data = config_template.replace("{port}", str(port)).replace(
        "{der_path}", str(i))
    config = open(config_path + "/config.yaml", "w")
    config.write(config_data)

with open("docker-compose.yaml", "w") as f:
    f.write(compose_template)
