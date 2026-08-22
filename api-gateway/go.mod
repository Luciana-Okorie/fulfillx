module github.com/luciana-okorie/fulfillx/api-gateway

go 1.22

require (
	github.com/go-chi/chi/v5 v5.0.12
	github.com/gorilla/websocket v1.5.1
	github.com/segmentio/kafka-go v0.4.47
	go.opentelemetry.io/otel v1.27.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.52.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.27.0
	go.opentelemetry.io/otel/sdk v1.27.0
	go.opentelemetry.io/otel/trace v1.27.0
)
