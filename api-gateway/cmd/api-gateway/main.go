package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/luciana-okorie/fulfillx/api-gateway/internal/proxy"
	"github.com/luciana-okorie/fulfillx/api-gateway/internal/telemetry"
	"github.com/luciana-okorie/fulfillx/api-gateway/internal/wsstream"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.Init(ctx, "api-gateway")
	if err != nil {
		log.Fatalf("telemetry init: %v", err)
	}
	defer shutdownTracing(context.Background())

	orderServiceURL := envOr("ORDER_SERVICE_URL", "http://order-service:8080")
	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	downstreamTopics := strings.Split(envOr("KAFKA_TOPICS_DOWNSTREAM",
		"inventory-events,payment-events,fulfillment-events"), ",")
	port := envOr("PORT", "8000")

	orderProxy, err := proxy.NewOrderServiceProxy(orderServiceURL)
	if err != nil {
		log.Fatalf("proxy config: %v", err)
	}

	hub := wsstream.NewHub()
	// A distinct consumer group per gateway replica (or a shared one
	// with more partitions) keeps this scalable horizontally without
	// stepping on the status-consumer or payment/fulfillment
	// consumer groups, which read the same topics for different
	// purposes.
	wsConsumer := wsstream.NewConsumer(kafkaBrokers, downstreamTopics, "api-gateway-ws", hub)
	go wsConsumer.Run(ctx)

	r := chi.NewRouter()

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// REST traffic: forward to Order Service. All idempotency and
	// rate-limit enforcement happens there, not duplicated here.
	r.Handle("/orders", orderProxy)
	r.Handle("/orders/*", orderProxy)

	// Real-time order status: ws://gateway/ws/orders/{id}
	r.Get("/ws/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "id")
		hub.ServeWS(orderID)(w, r)
	})

	// The REST surface (proxied to Order Service) gets an HTTP server
	// span per request via otelhttp, continuing whatever trace the
	// client started (or beginning a new one). The WebSocket upgrade
	// route is intentionally NOT wrapped this way — a span that spans
	// an entire long-lived WebSocket connection isn't a meaningful
	// unit of tracing, and the events it forwards are already traced
	// at their point of origin (each service's outbox worker).
	srv := &http.Server{Addr: ":" + port, Handler: otelhttp.NewHandler(r, "api-gateway")}
	go func() {
		log.Printf("api-gateway listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
