#!/bin/sh
# Installs the Surfra supervisor.
#   RUN curl -fsSL https://surfra.io/install.sh | sh
#   RUN wget -qO- https://surfra.io/install.sh | sh
# SURFRA_VERSION picks a release (default: latest). SURFRA_INSTALL_DIR picks the folder (default: /usr/local/bin).
set -eu

repo="surfra-io/surfra-worker"
version="${SURFRA_VERSION:-latest}"
dir="${SURFRA_INSTALL_DIR:-/usr/local/bin}"

case "$(uname -s)" in
  Linux) ;;
  *) echo "surfra: the supervisor runs on Linux containers, not $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) echo "surfra: no build for $(uname -m)" >&2; exit 1 ;;
esac

if [ "$version" = "latest" ]; then
  base="https://github.com/$repo/releases/latest/download"
else
  base="https://github.com/$repo/releases/download/$version"
fi

fetch() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$2" "$1"
  else
    echo "surfra: install needs curl or wget. In images without either, use:" >&2
    echo "  ARG TARGETARCH" >&2
    echo "  ADD --chmod=755 $base/surfra-linux-\${TARGETARCH} /usr/local/bin/surfra" >&2
    exit 1
  fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fetch "$base/surfra-linux-$arch" "$tmp/surfra"
fetch "$base/checksums.txt" "$tmp/checksums.txt"

want="$(grep " surfra-linux-$arch\$" "$tmp/checksums.txt" | cut -d' ' -f1)"
if command -v sha256sum >/dev/null 2>&1; then
  got="$(sha256sum "$tmp/surfra" | cut -d' ' -f1)"
elif command -v shasum >/dev/null 2>&1; then
  got="$(shasum -a 256 "$tmp/surfra" | cut -d' ' -f1)"
else
  got="$want"
fi
if [ -z "$want" ] || [ "$got" != "$want" ]; then
  echo "surfra: checksum mismatch for surfra-linux-$arch" >&2
  exit 1
fi

mkdir -p "$dir"
install -m 0755 "$tmp/surfra" "$dir/surfra" 2>/dev/null || { cp "$tmp/surfra" "$dir/surfra" && chmod 0755 "$dir/surfra"; }
echo "surfra $("$dir/surfra" version) installed to $dir/surfra"
