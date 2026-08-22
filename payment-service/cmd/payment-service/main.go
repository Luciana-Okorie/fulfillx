package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/luciana-okorie/fulfillx/payment-service/internal/consumer"
	"github.com/luciana-okorie/fulfillx/payment-service/internal/db"
	"github.com/luciana-okorie/fulfillx/payment-service/internal/outbox"
	"github.com/luciana-okorie/fulfillx/payment-service/internal/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.Init(ctx, "payment-service")
	if err != nil {
		log.Fatalf("telemetry init: %v", err)
	}
	defer shutdownTracing(context.Background())

	dbURL := envOr("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/fulfillx_payment?sslmode=disable")
	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	inTopic := envOr("KAFKA_TOPIC_INVENTORY_EVENTS", "inventory-events")
	outTopic := envOr("KAFKA_TOPIC_PAYMENT_EVENTS", "payment-events")
	groupID := envOr("KAFKA_CONSUMER_GROUP", "payment-service")
	port := envOr("PORT", "8082")

	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	repo := db.NewPaymentRepo(pool)

	c := consumer.New(kafkaBrokers, inTopic, groupID, repo)
	go c.Run(ctx)

	worker := outbox.NewWorker(pool, kafkaBrokers, outTopic)
	go worker.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		log.Printf("payment-service health server on :%s", port)
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
