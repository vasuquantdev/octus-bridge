#!/usr/bin/env bash
mkdir "artifacts"
RELAY_VERSION=$(grep version Cargo.toml | head -n 1 | awk -F '=' '{print $2}' | tr -d '"' | tr -d ' ')

echo "Building intermediate dockerfile"
sudo docker build . -f deb.Dockerfile -t builder_deb
echo "Successfully built"
docker create -ti --name dummy_build builder_deb /bin/bash
DEB_FILE="relay_$(echo -n "$RELAY_VERSION")_amd64.deb"
docker cp "dummy_build:/relay/target/debian/$DEB_FILE" "artifacts/$DEB_FILE"
docker rm -f dummy_build
echo "You can find deb file in: artifacts/$DEB_FILE"
