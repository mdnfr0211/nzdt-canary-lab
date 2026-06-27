package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// event mirrors the gateway envelope. The worker re-decodes payload and may
// apply schema checks. NOTE: the worker validates the SAME envelope shape the
// gateway accepted, plus (when strict) the deep-payload contract that scenario
// 2's bad-payload generator violates.
type event struct {
	UserID  string          `json:"user_id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// strictPayload is the "new schema" v2 enforces. v1 (tolerant) ignores these.
// Scenario 2's bad generator omits `item_id`, which v2 marks as required.
type strictPayload struct {
	ItemID   string `json:"item_id"`
	Amount   int    `json:"amount"`
	Category string `json:"category"`
}

type config struct {
	kafkaBrokers string
	kafkaTopic   string
	kafkaGroup   string
	otlpEndpoint string
	appVersion   string
	schemaStrict bool
}

func loadConfig() config {
	return config{
		kafkaBrokers: getenv("KAFKA_BROKERS", "my-cluster-kafka-bootstrap.kafka.svc:9092"),
		kafkaTopic:   getenv("KAFKA_TOPIC", "events"),
		kafkaGroup:   getenv("KAFKA_GROUP", "workers"),
		otlpEndpoint: getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "adot-collector.monitoring.svc:4317"),
		appVersion:   getenv("APP_VERSION", "v1"),
		schemaStrict: getenv("SCHEMA_STRICT", "false") == "true",
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mp, err := initMeterProvider(ctx, cfg)
	if err != nil {
		log.Fatalf("init metrics: %v", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mp.Shutdown(shutCtx)
	}()

	meter := otel.Meter("event-worker")
	attrs := []attribute.KeyValue{attribute.String("version", cfg.appVersion)}
	processedTotal, _ := meter.Int64Counter("worker_events_processed_total",
		metric.WithDescription("Events consumed from Kafka"))
	failedTotal, _ := meter.Int64Counter("worker_events_failed_total",
		metric.WithDescription("Events rejected by schema/validation (canary failure trigger)"))
	duration, _ := meter.Float64Histogram("worker_event_process_duration_seconds",
		metric.WithDescription("Per-event processing duration"))

	// One reader per worker pod, joining the SHARED consumer group `workers`.
	// Both v1 and v2 join the same group -> partitions are shared across all
	// members; Kafka assigns ~canaryReplicas/total partitions to canary pods.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(cfg.kafkaBrokers, ","),
		GroupID:  cfg.kafkaGroup,
		Topic:    cfg.kafkaTopic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer r.Close()

	log.Printf("event-worker %s starting (group=%s topic=%s strict=%v)",
		cfg.appVersion, cfg.kafkaGroup, cfg.kafkaTopic, cfg.schemaStrict)

	for {
		if err := consume(ctx, r, cfg, processedTotal, failedTotal, duration, attrs); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("consume loop error: %v (retrying in 2s)", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func consume(ctx context.Context, r *kafka.Reader, cfg config,
	processed, failed metric.Int64Counter,
	dur metric.Float64Histogram,
	attrs []attribute.KeyValue,
) error {
	// FetchMessage lets us commit manually after processing, so a rejected
	// (DLQ-logged) message still commits and is not redelivered -> no shared-
	// state corruption, no poison-message loop.
	m, err := r.FetchMessage(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	if perr := process(ctx, m, cfg, processed, failed, attrs); perr != nil {
		// Failure path: increment failed counter, log to DLQ path (stdout).
		failed.Add(ctx, 1, metric.WithAttributes(attrs...))
		log.Printf("[DLQ] version=%s partition=%d offset=%d key=%s reject=%v",
			cfg.appVersion, m.Partition, m.Offset, string(m.Key), perr)
	}

	processed.Add(ctx, 1, metric.WithAttributes(attrs...))
	dur.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))

	if err := r.CommitMessages(ctx, m); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// process validates one message. The error returned here is what the worker
// failure injection hinges on.
func process(ctx context.Context, m kafka.Message, cfg config,
	processed, failed metric.Int64Counter, attrs []attribute.KeyValue) error {
	var ev event
	if err := json.Unmarshal(m.Value, &ev); err != nil {
		return fmt.Errorf("malformed envelope")
	}
	if !cfg.schemaStrict {
		return nil // v1 tolerant path: accept anything with a valid envelope
	}

	// v2 strict path: enforce the new deep schema. Scenario 2's bad generator
	// omits `item_id` -> this rejects and returns the DLQ error.
	var p strictPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return fmt.Errorf("payload unmarshal: %w", err)
	}
	if p.ItemID == "" {
		return fmt.Errorf("missing required field: item_id")
	}
	// Happy path: write to sink (log-only for demo).
	if ev.Type == "buy" && p.Amount > 0 {
		_ = p // sink write elided (log-only)
	}
	return nil
}

func initMeterProvider(ctx context.Context, cfg config) (*sdkmetric.MeterProvider, error) {
	exp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(cfg.otlpEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp metric exporter: %w", err)
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("event-worker"),
			semconv.ServiceVersion(cfg.appVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(15*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	return mp, nil
}
