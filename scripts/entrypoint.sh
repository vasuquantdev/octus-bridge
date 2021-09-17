#!/bin/bash

wget \
  -O ton-global.config.json \
  https://raw.githubusercontent.com/tonlabs/main.ton.dev/master/configs/main.ton.dev/ton-global.config.json

./relay \
  --config /cfg/config.yaml \
  run \
  --global-config ton-global.config.json
