// Command loadtest is a minimal concurrent load generator for AdSelector's
// POST /v1/select endpoint. It exists to produce the "p99 latency at N
// qps" numbers referenced in the project README's marketing angle.
//
// NOTE: this has intentionally never been run as part of scaffolding this
// project -- see repo README for the commands that WOULD be run once the
// server, Redis, and Kafka are actually up (`docker compose up`).
//
// scripts/loadtest.js is a k6-based alternative to this program for anyone
// who prefers k6's reporting/UI over this bespoke Go harness.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/michaelstone897/adselector/pkg/client"
)

func main() {
	var (
		baseURL     = flag.String("url", "http://localhost:8080", "AdSelector base URL")
		qps         = flag.Int("qps", 200, "target requests per second")
		duration    = flag.Duration("duration", 30*time.Second, "how long to run the load test")
		concurrency = flag.Int("concurrency", 50, "max in-flight requests")
	)
	flag.Parse()

	c := client.New(*baseURL)

	geos := []string{"US", "DE", "FR", "CA", "BR"}
	devices := []string{"mobile", "desktop", "ctv"}

	var (
		mu        sync.Mutex
		latencies []time.Duration
		errCount  int
		filled    int
		noFill    int
	)

	sem := make(chan struct{}, *concurrency)
	ticker := time.NewTicker(time.Second / time.Duration(*qps))
	defer ticker.Stop()

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup

	fmt.Printf("loadtest: target=%s qps=%d concurrency=%d duration=%s\n", *baseURL, *qps, *concurrency, *duration)

	for time.Now().Before(deadline) {
		<-ticker.C
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			req := client.SelectRequest{
				UserID:   fmt.Sprintf("user-%d", rand.Intn(10_000)),
				AdSlotID: "slot-homepage-top",
				Geo:      geos[rand.Intn(len(geos))],
				Device:   devices[rand.Intn(len(devices))],
			}

			start := time.Now()
			resp, err := c.Select(context.Background(), req)
			elapsed := time.Since(start)

			mu.Lock()
			defer mu.Unlock()
			latencies = append(latencies, elapsed)
			switch {
			case err != nil:
				errCount++
			case resp.Filled:
				filled++
			default:
				noFill++
			}
		}()
	}

	wg.Wait()
	report(latencies, filled, noFill, errCount)
}

func report(latencies []time.Duration, filled, noFill, errCount int) {
	if len(latencies) == 0 {
		fmt.Println("no requests completed")
		os.Exit(1)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	pct := func(p float64) time.Duration {
		idx := int(p * float64(len(latencies)))
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return latencies[idx]
	}

	fmt.Printf("requests: %d (filled=%d no_fill=%d errors=%d)\n", len(latencies), filled, noFill, errCount)
	fmt.Printf("latency: p50=%s p90=%s p99=%s max=%s\n",
		pct(0.50), pct(0.90), pct(0.99), latencies[len(latencies)-1])
}
