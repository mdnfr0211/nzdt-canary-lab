package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
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

type config struct {
	kafkaBrokers string
	kafkaTopic   string
	otlpEndpoint string
	appVersion   string
	listenAddr   string
}

func loadConfig() config {
	return config{
		kafkaBrokers: getenv("KAFKA_BROKERS", "my-cluster-kafka-bootstrap.kafka.svc:9092"),
		kafkaTopic:   getenv("KAFKA_TOPIC", "events"),
		otlpEndpoint: getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "adot-collector.monitoring.svc:4317"),
		appVersion:   getenv("APP_VERSION", "v1"),
		listenAddr:   getenv("LISTEN_ADDR", ":8080"),
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

	meter := otel.Meter("event-gateway")
	requestsTotal, _ := meter.Int64Counter("gateway_requests_total",
		metric.WithDescription("Total events received by the gateway"))
	produceErrors, _ := meter.Int64Counter("gateway_produce_errors_total",
		metric.WithDescription("Kafka produce failures"))
	requestDuration, _ := meter.Float64Histogram("gateway_request_duration_seconds",
		metric.WithDescription("HTTP /event handling duration"))

	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(cfg.kafkaBrokers, ",")...),
		Topic:        cfg.kafkaTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    1,
	}
	defer writer.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		metricAttrs := metric.WithAttributes(attribute.String("version", cfg.appVersion))
		defer requestsTotal.Add(ctx, 1, metricAttrs)

		var evt event
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if evt.UserID == "" || (evt.Type != "click" && evt.Type != "buy") {
			http.Error(w, "invalid envelope: user_id and type (click|buy) required", http.StatusBadRequest)
			return
		}

		message := kafka.Message{
			Key:   []byte(evt.UserID),
			Value: mustJSON(evt),
		}
		writeCtx, cancelWrite := context.WithTimeout(ctx, 5*time.Second)
		defer cancelWrite()
		if err := writer.WriteMessages(writeCtx, message); err != nil {
			produceErrors.Add(ctx, 1, metricAttrs)
			http.Error(w, "produce failed", http.StatusBadGateway)
			return
		}

		requestDuration.Record(ctx, time.Since(start).Seconds(), metricAttrs)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "# OTLP metrics push to %s (app=%s, version=%s)\n",
			cfg.otlpEndpoint, "event-gateway", cfg.appVersion)
	})

	server := &http.Server{Addr: cfg.listenAddr, Handler: mux}
	go func() {
		log.Printf("event-gateway %s listening on %s (kafka=%s topic=%s)",
			cfg.appVersion, cfg.listenAddr, cfg.kafkaBrokers, cfg.kafkaTopic)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = server.Shutdown(shutdownCtx)
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
			semconv.ServiceName("event-gateway"),
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

func mustJSON(evt event) []byte {
	payload, _ := json.Marshal(evt)
	return payload
}
