# Static supervisor. The image has no shell and no Python.
# Arm64 is built beside this file:
#   CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags '-s -w -X main.version=0.2.1' -o dist/surfra-linux-arm64 ./cmd/surfra
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w -X main.version=0.2.1' -o /out/surfra ./cmd/surfra

FROM scratch
COPY --from=build /out/surfra /surfra
ENTRYPOINT ["/surfra"]
CMD ["worker"]
