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

// event is the envelope the gateway validates. The deep payload is intentionally
// not validated here: the gateway must stay healthy even when the worker rejects
// bad payloads (scenario 2).
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

	meter := otel.Meter("event-gateway")
	requestsTotal, _ := meter.Int64Counter("gateway_requests_total",
		metric.WithDescription("Total events received by the gateway"))
	produceErrors, _ := meter.Int64Counter("gateway_produce_errors_total",
		metric.WithDescription("Kafka produce failures"))
	duration, _ := meter.Float64Histogram("gateway_request_duration_seconds",
		metric.WithDescription("HTTP /event handling duration"))

	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(cfg.kafkaBrokers, ",")...),
		Topic:        cfg.kafkaTopic,
		Balancer:     &kafka.Hash{}, // key = user_id -> sticky partitioning
		RequiredAcks: kafka.RequireAll,
		BatchSize:    1, // demo: send each event promptly for predictable canary load
	}
	defer writer.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// ready as long as the writer is constructed; Kafka liveness is the
		// canary's concern, not the readiness gate.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		attrs := []attribute.KeyValue{attribute.String("version", cfg.appVersion)}

		var ev event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		// Envelope validation only. Only user_id + type are required.
		if ev.UserID == "" || (ev.Type != "click" && ev.Type != "buy") {
			requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			http.Error(w, "invalid envelope: user_id and type (click|buy) required", http.StatusBadRequest)
			return
		}

		// Produce to Kafka with key = user_id (sticky).
		msg := kafka.Message{
			Key:   []byte(ev.UserID),
			Value: mustJSON(ev),
		}
		wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
		defer wcancel()
		if err := writer.WriteMessages(wctx, msg); err != nil {
			produceErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
			requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			http.Error(w, "produce failed", http.StatusBadGateway)
			return
		}

		requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
		duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	})

	// /metrics is informational; primary metrics path is OTLP push to ADOT.
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "# OTLP metrics push to %s (app=%s, version=%s)\n",
			cfg.otlpEndpoint, "event-gateway", cfg.appVersion)
	})

	srv := &http.Server{Addr: cfg.listenAddr, Handler: mux}
	go func() {
		log.Printf("event-gateway %s listening on %s (kafka=%s topic=%s)",
			cfg.appVersion, cfg.listenAddr, cfg.kafkaBrokers, cfg.kafkaTopic)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	_ = srv.Shutdown(sctx)
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
			semconv.ServiceName("event-gateway"),
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

func mustJSON(v event) []byte {
	b, _ := json.Marshal(v)
	return b
}
