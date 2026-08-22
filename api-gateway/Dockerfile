FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod go.sum* ./
COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/api-gateway ./cmd/api-gateway

FROM alpine:3.19

RUN adduser -D -u 10001 appuser

COPY --from=builder /out/api-gateway /usr/local/bin/api-gateway

USER appuser

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/api-gateway"]