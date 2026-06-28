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

type event struct {
	UserID  string          `json:"user_id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

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

func getenv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	meterProvider, err := initMeterProvider(ctx, cfg)
	if err != nil {
		log.Fatalf("init metrics: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = meterProvider.Shutdown(shutdownCtx)
	}()

	meter := otel.Meter("event-worker")
	metricAttrs := []attribute.KeyValue{attribute.String("version", cfg.appVersion)}
	processedTotal, _ := meter.Int64Counter("worker_events_processed_total",
		metric.WithDescription("Events consumed from Kafka"))
	failedTotal, _ := meter.Int64Counter("worker_events_failed_total",
		metric.WithDescription("Events rejected by schema/validation (canary failure trigger)"))
	processDuration, _ := meter.Float64Histogram("worker_event_process_duration_seconds",
		metric.WithDescription("Per-event processing duration"))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(cfg.kafkaBrokers, ","),
		GroupID:  cfg.kafkaGroup,
		Topic:    cfg.kafkaTopic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Printf("event-worker %s starting (group=%s topic=%s strict=%v)",
		cfg.appVersion, cfg.kafkaGroup, cfg.kafkaTopic, cfg.schemaStrict)

	for {
		if err := consume(ctx, reader, cfg, processedTotal, failedTotal, processDuration, metricAttrs); err != nil {
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

func consume(ctx context.Context, reader *kafka.Reader, cfg config,
	processedTotal, failedTotal metric.Int64Counter,
	processDuration metric.Float64Histogram,
	metricAttrs []attribute.KeyValue,
) error {

	message, err := reader.FetchMessage(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	if processErr := process(message, cfg); processErr != nil {

		failedTotal.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		log.Printf("[DLQ] version=%s partition=%d offset=%d key=%s reject=%v",
			cfg.appVersion, message.Partition, message.Offset, string(message.Key), processErr)
	}

	processedTotal.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
	processDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(metricAttrs...))

	if err := reader.CommitMessages(ctx, message); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func process(message kafka.Message, cfg config) error {
	var evt event
	if err := json.Unmarshal(message.Value, &evt); err != nil {
		return fmt.Errorf("malformed envelope")
	}
	if !cfg.schemaStrict {
		return nil
	}

	var payload strictPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("payload unmarshal: %w", err)
	}
	if payload.ItemID == "" {
		return fmt.Errorf("missing required field: item_id")
	}

	if evt.Type == "buy" && payload.Amount > 0 {
		_ = payload
	}
	return nil
}

func initMeterProvider(ctx context.Context, cfg config) (*sdkmetric.MeterProvider, error) {
	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(cfg.otlpEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp metric exporter: %w", err)
	}
	serviceResource, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("event-worker"),
			semconv.ServiceVersion(cfg.appVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(15*time.Second))),
		sdkmetric.WithResource(serviceResource),
	)
	otel.SetMeterProvider(meterProvider)
	return meterProvider, nil
}
