#!/bin/sh
# Builds both Linux binaries and checksums.txt into dist/, then publishes a GitHub release.
#   ./release.sh v0.2.0
set -eu
tag="$1"
version="${tag#v}"
rm -rf dist && mkdir -p dist
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-buildvcs=false golang:1.23-bookworm sh -c "
  for arch in amd64 arm64; do
    CGO_ENABLED=0 GOOS=linux GOARCH=\$arch go build -trimpath -ldflags '-s -w -X main.version=$version' -o dist/surfra-linux-\$arch ./cmd/surfra
  done
  chown -R $(id -u):$(id -g) dist"
(cd dist && sha256sum surfra-linux-amd64 surfra-linux-arm64 > checksums.txt)
cp install.sh dist/install.sh
gh release create "$tag" dist/surfra-linux-amd64 dist/surfra-linux-arm64 dist/checksums.txt dist/install.sh --title "$tag" --notes "Install: RUN curl -fsSL https://surfra.io/install.sh | sh"
