# NOTE: this Dockerfile has not been built or tested as part of scaffolding
# this project. It is written to produce a correct image once `go mod
# tidy` has been run locally (so go.sum exists) -- see repo README.

FROM golang:1.24-bookworm AS build
WORKDIR /src

COPY go.mod ./
# go.sum intentionally not copied here -- it does not exist yet because
# `go mod tidy` has not been run against this module.
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/adselector ./cmd/server

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/adselector /adselector
EXPOSE 8080
ENTRYPOINT ["/adselector"]
