FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod go.sum* ./
COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/order-service ./cmd/order-service

FROM alpine:3.19

RUN adduser -D -u 10001 appuser

COPY --from=builder /out/order-service /usr/local/bin/order-service

USER appuser

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/order-service"]