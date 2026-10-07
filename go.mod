module github.com/michaelstone897/adselector

go 1.24

// NOTE: `go mod tidy` has not been run against this module yet. The
// versions below are plausible recent releases pinned by hand while
// scaffolding this project; go.sum and the full indirect dependency graph
// (protobuf, golang.org/x/sys, etc. pulled in transitively by these) have
// not been generated. Run `go mod tidy` before building.
require (
	github.com/go-chi/chi/v5 v5.1.0
	github.com/prometheus/client_golang v1.20.5
	github.com/redis/go-redis/v9 v9.7.0
	github.com/segmentio/kafka-go v0.4.47
)
