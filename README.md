# ETH-to-TON relay


## How to run

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
