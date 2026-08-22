package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/luciana-okorie/fulfillx/order-service/internal/db"
	"github.com/luciana-okorie/fulfillx/order-service/internal/handlers"
	"github.com/luciana-okorie/fulfillx/order-service/internal/idempotency"
	"github.com/luciana-okorie/fulfillx/order-service/internal/outbox"
	"github.com/luciana-okorie/fulfillx/order-service/internal/statusconsumer"
	"github.com/luciana-okorie/fulfillx/order-service/internal/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.Init(ctx, "order-service")
	if err != nil {
		log.Fatalf("telemetry init: %v", err)
	}
	defer shutdownTracing(context.Background())

	dbURL := envOr("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/fulfillx?sslmode=disable")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := envOr("KAFKA_TOPIC_ORDER_EVENTS", "order-events")
	port := envOr("PORT", "8080")

	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis connect: %v", err)
	}
	defer rdb.Close()

	orderRepo := db.NewOrderRepo(pool)
	idemChecker := idempotency.NewChecker(rdb)
	rateLimiter := idempotency.NewRateLimiter(rdb)
	orderHandler := handlers.NewOrderHandler(orderRepo, idemChecker)

	// Outbox worker runs in-process for simplicity. In production
	// this would be its own deployable so it scales/restarts
	// independently of the API — noted as a trade-off in the README.
	worker := outbox.NewWorker(pool, kafkaBrokers, kafkaTopic)
	go worker.Run(ctx)

	// Consumes downstream events from inventory/payment/fulfillment so
	// GET /orders/:id/status reflects the order's true current state
	// without the client needing to know about the other three
	// services.
	downstreamTopics := strings.Split(envOr("KAFKA_TOPICS_DOWNSTREAM",
		"inventory-events,payment-events,fulfillment-events"), ",")
	statusC := statusconsumer.New(kafkaBrokers, downstreamTopics, "order-service-status", orderRepo)
	go statusC.Run(ctx)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(10 * time.Second))

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	r.Route("/", func(r chi.Router) {
		r.Use(rateLimiter.Middleware)
		orderHandler.Routes(r)
	})

	srv := &http.Server{Addr: ":" + port, Handler: otelhttp.NewHandler(r, "order-service")}

	go func() {
		log.Printf("order-service listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
