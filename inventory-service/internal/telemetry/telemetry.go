// Package telemetry wires up OpenTelemetry tracing for this service.
// Each service in FulfillX carries its own copy of this file rather
// than importing a shared module — same reasoning as the duplicated
// outbox/processed_events pattern (see docs/architecture.md): it
// keeps each service deployable and versionable independently.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Init configures a global TracerProvider that exports spans via OTLP
// over HTTP. Returns a shutdown func to flush on exit. If
// OTEL_EXPORTER_OTLP_ENDPOINT is unset, tracing degrades to a no-op
// exporter rather than failing service startup — observability
// should never be a hard dependency for the business logic to run.
func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		otel.SetTextMapPropagator(propagation.TraceContext{})
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating otlp exporter: %w", err)
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	// W3C traceparent format — this is what lets us carry trace
	// context inside a Postgres text column (the outbox's
	// trace_context field) and inside a Kafka message header, and
	// have any OTel-instrumented consumer understand it.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}

// Tracer is a small convenience so call sites don't need to repeat
// the instrumentation-name string at every otel.Tracer(...) call.
func Tracer(serviceName string) trace.Tracer {
	return otel.Tracer(serviceName)
}
