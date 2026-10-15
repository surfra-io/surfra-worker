#!/bin/sh
# Builds both Linux binaries, deb/rpm/apk packages, and checksums.txt into dist/, then publishes a GitHub release.
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
for arch in amd64 arm64; do
  sed "s/\${ARCH}/$arch/g; s/\${VERSION}/$version/g" nfpm.yaml > "dist/nfpm-$arch.yaml"
  for format in deb rpm apk; do
    docker run --rm -u "$(id -u):$(id -g)" -v "$PWD":/src -w /src goreleaser/nfpm package -f "dist/nfpm-$arch.yaml" -p "$format" -t dist/
  done
  rm "dist/nfpm-$arch.yaml"
done
(cd dist && sha256sum surfra-linux-* *.deb *.rpm *.apk > checksums.txt)
cp install.sh dist/install.sh
gh release create "$tag" dist/surfra-linux-* dist/*.deb dist/*.rpm dist/*.apk dist/checksums.txt dist/install.sh --title "$tag" \
  --notes "Install in a Dockerfile: RUN curl -fsSL https://surfra.io/install.sh | sh. Packages: .deb for Debian and Ubuntu, .rpm for Fedora, RHEL, and Amazon Linux, .apk for Alpine."
