# Build the analytics consumer. Run `go mod tidy` inside analytics/ first so
# go.sum exists.
FROM golang:1.24-bookworm AS build
WORKDIR /src

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w" \
    -o /out/adselector-analytics .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/adselector-analytics /adselector-analytics
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["/adselector-analytics"]
