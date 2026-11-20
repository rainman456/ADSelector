# Build the API only. The analytics consumer has its own Dockerfile.
# `go mod tidy` must have produced go.sum before building; see docs/verify.md.

FROM golang:1.24-bookworm AS build
WORKDIR /src

ARG VERSION=dev

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/adselector ./cmd/server

# static-debian12:nonroot already defines uid/gid 65532; the explicit USER
# keeps the non-root contract even if the base tag is changed.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/adselector /adselector
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/adselector"]
