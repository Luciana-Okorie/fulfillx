FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum* ./
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/payment-service ./cmd/payment-service

FROM alpine:3.19
RUN adduser -D -u 10001 appuser
COPY --from=builder /out/payment-service /usr/local/bin/payment-service
USER appuser
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/payment-service"]
