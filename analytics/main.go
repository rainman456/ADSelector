// Command analytics consumes impression_served events from Kafka and batch
// inserts them into ClickHouse. It is a separate Go module so the core
// ad-decisioning service never depends on analytics libraries (see
// docs/adr/0002-analytics-companion-module.md).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

// adEvent mirrors internal/events.ImpressionServed. It is redefined because
// this is a separate module and cannot import the service's internal package.
type adEvent struct {
	EventID        string    `json:"event_id"`
	ServedAt       time.Time `json:"served_at"`
	UserID         string    `json:"user_id"`
	AdSlotID       string    `json:"ad_slot_id"`
	CampaignID     string    `json:"campaign_id"`
	CreativeID     string    `json:"creative_id"`
	BidPriceMicros int64     `json:"bid_price_micros"`
	Geo            string    `json:"geo"`
	Device         string    `json:"device"`
}

type config struct {
	listenAddr       string
	brokers          []string
	topic            string
	group            string
	clickhouseAddr   string
	clickhouseDB     string
	clickhouseUser   string
	clickhousePass   string
	batchSize        int
	flushInterval    time.Duration
	shutdownGrace    time.Duration
	insertTimeout    time.Duration
	metricsNamespace string
}

func main() {
	cfg := loadConfig()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		consumed = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.metricsNamespace, Subsystem: "kafka", Name: "consumed_total",
			Help: "Kafka messages fetched from the impression_served topic.",
		})
		inserted = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.metricsNamespace, Subsystem: "clickhouse", Name: "inserted_total",
			Help: "Events successfully inserted into ClickHouse.",
		})
		failed = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.metricsNamespace, Subsystem: "analytics", Name: "failed_total",
			Help: "Events or messages that could not be decoded or inserted.",
		})
	)
	prometheus.MustRegister(consumed, inserted, failed)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.clickhouseAddr},
		Auth: clickhouse.Auth{
			Database: cfg.clickhouseDB,
			Username: cfg.clickhouseUser,
			Password: cfg.clickhousePass,
		},
		DialTimeout: 5 * time.Second,
		MaxOpenConns: 5,
		MaxIdleConns: 5,
	})
	if err != nil {
		logger.Error("failed to configure clickhouse", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	if err := conn.Ping(pingCtx); err != nil {
		cancelPing()
		logger.Error("clickhouse ping failed; apply migrations/0001_ad_events.sql first", "err", err)
		os.Exit(1)
	}
	cancelPing()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.brokers,
		GroupID:        cfg.group,
		Topic:          cfg.topic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		MaxWait:        time.Second,
		CommitInterval: 0, // commit manually only after a successful ClickHouse insert
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("analytics listening",
			"addr", cfg.listenAddr, "kafka_topic", cfg.topic, "clickhouse_db", cfg.clickhouseDB)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("analytics http server failed", "err", err)
			os.Exit(1)
		}
	}()

	runConsumer(ctx, logger, cfg, reader, conn, consumed, inserted, failed)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("analytics http shutdown failed", "err", err)
	}
	logger.Info("analytics stopped")
}

type fetched struct {
	msg kafka.Message
	err error
}

func runConsumer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	reader *kafka.Reader,
	conn clickhouse.Conn,
	consumed, inserted, failed prometheus.Counter,
) {
	messages := make(chan fetched)
	go func() {
		defer close(messages)
		for ctx.Err() == nil {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				select {
				case messages <- fetched{err: err}:
				case <-ctx.Done():
					return
				}
				time.Sleep(time.Second)
				continue
			}
			select {
			case messages <- fetched{msg: msg}:
			case <-ctx.Done():
				return
			}
		}
	}()

	batch := make([]adEvent, 0, cfg.batchSize)
	msgs := make([]kafka.Message, 0, cfg.batchSize)
	ticker := time.NewTicker(cfg.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		insertCtx, cancel := context.WithTimeout(context.Background(), cfg.insertTimeout)
		err := insertBatch(insertCtx, conn, batch)
		cancel()
		if err != nil {
			failed.Add(float64(len(batch)))
			logger.Error("clickhouse insert failed; exiting so uncommitted offsets replay", "count", len(batch), "err", err)
			os.Exit(1)
		}

		inserted.Add(float64(len(batch)))
		commitCtx, cancelCommit := context.WithTimeout(context.Background(), 10*time.Second)
		if err := reader.CommitMessages(commitCtx, msgs...); err != nil {
			logger.Warn("kafka offset commit failed; duplicates possible after restart", "count", len(msgs), "err", err)
		}
		cancelCommit()

		batch = batch[:0]
		msgs = msgs[:0]
	}

	for {
		select {
		case item, ok := <-messages:
			if !ok {
				flush()
				return
			}
			if item.err != nil {
				failed.Inc()
				logger.Warn("kafka fetch failed; retrying", "err", item.err)
				continue
			}

			consumed.Inc()
			var evt adEvent
			if err := json.Unmarshal(item.msg.Value, &evt); err != nil {
				failed.Inc()
				logger.Warn("dropping undecodable message", "offset", item.msg.Offset, "err", err)
				// A poison message must not block the partition forever.
				commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = reader.CommitMessages(commitCtx, item.msg)
				cancel()
				continue
			}

			batch = append(batch, evt)
			msgs = append(msgs, item.msg)
			if len(batch) >= cfg.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

func insertBatch(ctx context.Context, conn clickhouse.Conn, events []adEvent) error {
	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO ad_events (event_id, served_at, user_id, ad_slot_id, campaign_id, creative_id, bid_price_micros, geo, device)")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for _, evt := range events {
		if err := batch.Append(
			evt.EventID,
			evt.ServedAt,
			evt.UserID,
			evt.AdSlotID,
			evt.CampaignID,
			evt.CreativeID,
			evt.BidPriceMicros,
			evt.Geo,
			evt.Device,
		); err != nil {
			return fmt.Errorf("append event %s: %w", evt.EventID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

func loadConfig() config {
	listenAddr := flag.String("listen", env("LISTEN_ADDR", ":8081"), "HTTP listen address for healthz/metrics")
	kafkaBroker := flag.String("kafka-broker", env("KAFKA_BROKER", "localhost:9092"), "comma-separated Kafka brokers")
	kafkaTopic := flag.String("kafka-topic", env("KAFKA_TOPIC", "impression_served"), "Kafka topic to consume")
	kafkaGroup := flag.String("kafka-group", env("KAFKA_GROUP", "adselector-analytics"), "Kafka consumer group")
	clickhouseAddr := flag.String("clickhouse-addr", env("CLICKHOUSE_ADDR", "localhost:9000"), "ClickHouse native address")
	clickhouseDB := flag.String("clickhouse-db", env("CLICKHOUSE_DB", "adselector"), "ClickHouse database")
	clickhouseUser := flag.String("clickhouse-user", env("CLICKHOUSE_USER", "default"), "ClickHouse user")
	clickhousePass := flag.String("clickhouse-password", env("CLICKHOUSE_PASSWORD", ""), "ClickHouse password")
	batchSize := flag.Int("batch-size", envInt("BATCH_SIZE", 500), "maximum events per insert")
	flushInterval := flag.Duration("flush-interval", envDuration("FLUSH_INTERVAL", 5*time.Second), "maximum time before flushing a partial batch")
	shutdownGrace := flag.Duration("shutdown-grace", envDuration("SHUTDOWN_GRACE", 10*time.Second), "HTTP shutdown timeout")
	insertTimeout := flag.Duration("insert-timeout", envDuration("INSERT_TIMEOUT", 15*time.Second), "ClickHouse insert timeout")
	flag.Parse()

	return config{
		listenAddr:       *listenAddr,
		brokers:          splitCSV(*kafkaBroker),
		topic:            *kafkaTopic,
		group:            *kafkaGroup,
		clickhouseAddr:   *clickhouseAddr,
		clickhouseDB:     *clickhouseDB,
		clickhouseUser:   *clickhouseUser,
		clickhousePass:   *clickhousePass,
		batchSize:        *batchSize,
		flushInterval:    *flushInterval,
		shutdownGrace:    *shutdownGrace,
		insertTimeout:    *insertTimeout,
		metricsNamespace: "adselector_analytics",
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func splitCSV(value string) []string {
	out := make([]string, 0, 1)
	start := 0
	for i := 0; i <= len(value); i++ {
		if i == len(value) || value[i] == ',' {
			if i > start {
				out = append(out, value[start:i])
			}
			start = i + 1
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}
