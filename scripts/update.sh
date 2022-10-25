#!/usr/bin/env bash
set -eE

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
REPO_DIR=$(cd "${SCRIPT_DIR}/../" && pwd -P)

function print_help() {
  echo 'Usage: update.sh [OPTIONS]'
  echo ''
  echo 'Options:'
  echo '  -h,--help         Print this help message and exit'
  echo '  -f,--force        Clear "/var/db/relay" on update'
  echo '  -s,--sync         Restart "timesyncd" service'
  echo '  -r,--reset-adnl   Generate new ADNL keys'
  echo '  -t,--type TYPE    One of two types of installation:'
  echo '                    native - A little more complex way, but gives some'
  echo '                             performance gain and reduces the load.'
  echo '                    docker - The simplest way, but adds some overhead.'
  echo '                             Not recommended for machines with lower'
  echo '                             specs than required.'
}

force="false"
restart_timesyncd="false"
reset_adnl="false"
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
      -h|--help)
        print_help
        exit 0
      ;;
      -f|--force)
        force="true"
        shift # past argument
      ;;
      -s|--sync)
        restart_timesyncd="true"
        shift # past argument
      ;;
      -r|--reset-adnl)
        reset_adnl="true"
        shift # past argument
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

echo "INFO: stopping relay service"
sudo systemctl stop relay

if [[ "$force" == "true" ]]; then
  echo "INFO: removing relay db"
  sudo rm -rf /var/db/relay
else
  echo 'INFO: skipping "/var/db/relay" deletion'
fi

if [[ "$setup_type" == "native" ]]; then
  echo 'INFO: running update for native installation'

  echo 'INFO: building relay'
  cd "$REPO_DIR"
  RUSTFLAGS="-C target_cpu=native" cargo build --release
  sudo cp "$REPO_DIR/target/release/relay" /usr/bin/relay

elif [[ "$setup_type" == "docker" ]]; then
  if ! sudo docker info > /dev/null 2>&1; then
    echo 'ERROR: This script uses docker, and it is not running or not configured properly.'
    echo '       Please start docker and try again'
    exit 1
  fi

  echo "INFO: pulling new image"
  image="gcr.io/broxus/ton/tonbridge/relay:master-$(git rev-parse --short=8 HEAD)"
  sudo docker pull "$image"
  sudo docker image tag "$image" relay

else
  echo 'ERROR: Unexpected'
  exit 1
fi

sudo wget -O /etc/relay/global.config.json \
  https://raw.githubusercontent.com/tonlabs/main.ton.dev/master/configs/ton-global.config.json

echo "INFO: preparing environment"
sudo mkdir -p /var/db/relay

if [[ "$restart_timesyncd" == "true" ]]; then
  echo 'INFO: restarting timesyncd'
  sudo systemctl restart systemd-timesyncd.service
fi

if [[ "$reset_adnl" == "true" ]]; then
  echo 'INFO: clearing ADNL keys'
  sudo rm -f /etc/relay/adnl-keys.json
fi

echo 'INFO: restarting relay service'
sudo systemctl restart relay

echo 'INFO: done'
echo ''
echo 'INFO: Systemd service: relay'
echo '      Keys and configs: /etc/relay'
echo '      Node DB and stuff: /var/db/relay'
echo ''
