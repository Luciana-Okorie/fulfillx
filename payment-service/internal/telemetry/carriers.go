package telemetry

import (
	"go.opentelemetry.io/otel/propagation"

	kafka "github.com/segmentio/kafka-go"
)

// KafkaHeaderCarrier adapts a *[]kafka.Header to OTel's
// TextMapCarrier so trace context can ride inside Kafka message
// headers (the standard W3C "traceparent" key) alongside the
// event_type header every consumer already reads.
type KafkaHeaderCarrier struct {
	Headers *[]kafka.Header
}

var _ propagation.TextMapCarrier = KafkaHeaderCarrier{}

func (c KafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c KafkaHeaderCarrier) Set(key, value string) {
	for i, h := range *c.Headers {
		if h.Key == key {
			(*c.Headers)[i].Value = []byte(value)
			return
		}
	}
	*c.Headers = append(*c.Headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.Headers))
	for _, h := range *c.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// TextCarrier adapts a single string (the outbox row's trace_context
// column) to TextMapCarrier so a span begun in the HTTP/consumer
// handler can be resumed later by the outbox worker — the two run in
// different goroutines, potentially minutes apart, so the only way to
// keep them in the same trace is to persist the traceparent alongside
// the event itself.
type TextCarrier struct {
	Value string
}

var _ propagation.TextMapCarrier = &TextCarrier{}

func (c *TextCarrier) Get(key string) string {
	if key == "traceparent" {
		return c.Value
	}
	return ""
}

func (c *TextCarrier) Set(key, value string) {
	if key == "traceparent" {
		c.Value = value
	}
}

func (c *TextCarrier) Keys() []string {
	if c.Value == "" {
		return nil
	}
	return []string{"traceparent"}
}
