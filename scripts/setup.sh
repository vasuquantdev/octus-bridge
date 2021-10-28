#!/usr/bin/env bash
set -eE

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
REPO_DIR=$(cd "${SCRIPT_DIR}/../" && pwd -P)

function print_help() {
  echo 'Usage: setup.sh [OPTIONS]'
  echo ''
  echo 'Options:'
  echo '  -h,--help         Print this help message and exit'
  echo '  -t,--type TYPE    One of two types of installation:'
  echo '                    native - A little more complex way, but gives some'
  echo '                             performance gain and reduces the load.'
  echo '                    docker - The simplest way, but adds some overhead.'
  echo '                             Not recommended for machines with lower'
  echo '                             specs than required.'
}

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
      -h|--help)
        print_help
        exit 0
      ;;
      -t|--type)
        setup_type="$2"
        shift # past argument
        if [ "$#" -gt 0 ]; then shift;
        else
          echo 'ERROR: Expected installation type'
          echo ''
          print_help
          exit 1
        fi
      ;;
      *) # unknown option
        echo 'ERROR: Unknown option'
        echo ''
        print_help
        exit 1
      ;;
  esac
done

if [[ "$setup_type" != "native" ]] && [[ "$setup_type" != "docker" ]]; then
  echo 'ERROR: Unknown installation type'
  echo ''
  print_help
  exit 1
fi

service_path="/etc/systemd/system/relay.service"
config_path="/etc/relay/config.yaml"

if [[ "$setup_type" == "native" ]]; then
  echo 'INFO: Running native installation'

  echo 'INFO: installing and updating dependencies'
  sudo apt update && sudo apt upgrade
  sudo apt install build-essential llvm clang

  echo 'INFO: installing Rust'
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  source "$HOME/.cargo/env"

  echo 'INFO: building relay'
  cd "$REPO_DIR"
  RUSTFLAGS="-C target_cpu=native" cargo build --release
  sudo cp "$REPO_DIR/target/release/relay" /usr/local/bin/relay

  echo 'INFO: creating systemd service'
  if [[ -f "$service_path" ]]; then
    echo "WARN: $service_path already exists"
  else
    sudo cp "$REPO_DIR/contrib/relay.native.service" "$service_path"
  fi

elif [[ "$setup_type" == "docker" ]]; then
  if ! sudo docker info > /dev/null 2>&1; then
    echo 'ERROR: This script uses docker, and it is not running or not configured properly.'
    echo '       Please start docker and try again'
    exit 1
  fi

  echo "INFO: pulling image"
  image="gcr.io/broxus/ton/tonbridge/relay:master-$(git rev-parse --short=8 HEAD)"
  sudo docker pull "$image"
  sudo docker image tag "$image" relay

  echo 'INFO: creating systemd service'
  if [[ -f "$service_path" ]]; then
    echo "WARN: $service_path already exists"
  else
    sudo cp "$REPO_DIR/contrib/relay.docker.service" "$service_path"
  fi
else
  echo 'ERROR: Unexpected'
  exit 1
fi

echo "INFO: preparing environment"
sudo mkdir -p /etc/relay
sudo mkdir -p /var/db/relay
if [[ -f "$config_path" ]]; then
  echo "WARN: $config_path already exists"
else
  sudo cp -n "$REPO_DIR/contrib/config.yaml" "$config_path"
fi
sudo wget -O /etc/relay/ton-global.config.json \
  https://raw.githubusercontent.com/tonlabs/main.ton.dev/master/configs/main.ton.dev/ton-global.config.json

echo 'INFO: done'
echo ''
echo 'INFO: Systemd service: relay'
echo '      Keys and configs: /etc/relay'
echo '      Node DB and stuff: /var/db/relay'
echo ''
echo 'NOTE: replace all "${..}" variables in /etc/relay/config.yaml'
echo '      or specify them in /etc/systemd/system/relay.service'
echo '      in "[Service]" section with something like this:'
echo '      Environment=RELAY_MASTER_KEY=stub-master-key'
