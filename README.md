# Surfra supervisor

A static Linux binary that runs inside your container on [Surfra](https://surfra.io). It starts your server, waits until it answers on localhost, forwards each request to it, and sends what your app prints to Surfra's log search. The replica token stays in this process, never in your app's environment.

## Install

Add one line to your Dockerfile:

```dockerfile
RUN curl -fsSL https://surfra.io/install.sh | sh
```

Images with `wget` instead of `curl`:

```dockerfile
RUN wget -qO- https://surfra.io/install.sh | sh
```

Images with neither, such as `python:3.12-slim`, distroless, or `scratch`:

```dockerfile
ARG TARGETARCH
ADD --chmod=755 https://github.com/surfra-io/surfra-worker/releases/latest/download/surfra-linux-${TARGETARCH} /usr/local/bin/surfra
```

Then start it as the container command. Surfra sets the rest of the environment when it places the machine.

```dockerfile
ENV SURFRA_ENTRYPOINT="python /app/server.py" SURFRA_HTTP_PORT=8000
ENTRYPOINT ["surfra", "worker"]
```

`SURFRA_VERSION=v0.2.1` pins a release. `SURFRA_INSTALL_DIR` changes where the binary goes. The script checks each download against the release's `checksums.txt`.

Each release also has packages for amd64 and arm64 that install `/usr/bin/surfra`:

```dockerfile
# Debian and Ubuntu
ARG TARGETARCH
ADD https://github.com/surfra-io/surfra-worker/releases/download/v0.2.1/surfra_0.2.1_${TARGETARCH}.deb /tmp/surfra.deb
RUN dpkg -i /tmp/surfra.deb && rm /tmp/surfra.deb
```

Fedora, RHEL, and Amazon Linux use `surfra-0.2.1-1.x86_64.rpm` or `surfra-0.2.1-1.aarch64.rpm` with `rpm -i`. Alpine uses `surfra_0.2.1_x86_64.apk` or `surfra_0.2.1_aarch64.apk` with `apk add --allow-untrusted`.

## Logs

Every line your app writes to stdout or stderr is still printed to the container output, and is also kept with a UTC timestamp and, when Surfra can tell, the request it belongs to. Your app gets that request ID in the `X-Surfra-Request-Id` header. Lines are buffered on local disk, up to 5% of it, and sent about once a minute. `SURFRA_LOGS=0` turns this off.

## Build

```bash
CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o surfra ./cmd/surfra
```

## License

Apache 2.0. See [LICENSE](LICENSE).
