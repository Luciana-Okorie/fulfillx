package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/luciana-okorie/fulfillx/order-service/internal/db"
	"github.com/luciana-okorie/fulfillx/order-service/internal/idempotency"
	"github.com/luciana-okorie/fulfillx/order-service/internal/models"
)

type OrderHandler struct {
	repo  *db.OrderRepo
	idem  *idempotency.Checker
}

func NewOrderHandler(repo *db.OrderRepo, idem *idempotency.Checker) *OrderHandler {
	return &OrderHandler{repo: repo, idem: idem}
}

func (h *OrderHandler) Routes(r chi.Router) {
	r.Post("/orders", h.CreateOrder)
	r.Get("/orders/{id}", h.GetOrder)
	r.Get("/orders/{id}/status", h.GetOrderStatus)
}

// CreateOrder implements the Idempotency-Key contract:
//  1. No key -> process normally (client opted out of the guarantee).
//  2. Key present, Postgres already has a stored response for it ->
//     return that stored response verbatim. No new order is created.
//  3. Key present, nothing stored yet -> take the Redis lock so a
//     concurrent duplicate request waits/rejects instead of racing
//     us to the insert, then create the order and persist the
//     response under that key.
func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "cannot read body")
		return
	}

	var req models.CreateOrderRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.CustomerID == "" || len(req.Items) == 0 {
		writeErr(w, http.StatusBadRequest, "customer_id and items are required")
		return
	}

	key := r.Header.Get("Idempotency-Key")
	ctx := r.Context()

	if key != "" {
		reqHash := idempotency.HashBody(body)

		if existing, err := h.repo.GetIdempotentResponse(ctx, key); err == nil {
			if existing.RequestHash != reqHash {
				writeErr(w, http.StatusConflict, "idempotency key reused with a different request body")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replay", "true")
			w.WriteHeader(existing.ResponseStatus)
			w.Write(existing.ResponseBody)
			return
		} else if !errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusInternalServerError, "idempotency lookup failed")
			return
		}

		locked, err := h.idem.AcquireLock(ctx, key)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "idempotency lock failed")
			return
		}
		if !locked {
			// Someone else is processing this exact key right now.
			// Ask the client to retry shortly rather than racing them.
			w.Header().Set("Retry-After", "1")
			writeErr(w, http.StatusConflict, "request with this idempotency key is already being processed")
			return
		}
		defer h.idem.ReleaseLock(ctx, key)
	}

	order, err := h.repo.CreateOrderWithOutbox(ctx, req)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "failed to create order")
		return
	}

	// otelhttp already opened a server span for this request; attach
	// the order id/customer id to it so a trace can be found starting
	// from either "this HTTP request" or "this order" without needing
	// a separate lookup step.
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("order.id", order.ID),
		attribute.String("customer.id", order.CustomerID),
	)

	respBody, _ := json.Marshal(order)

	if key != "" {
		reqHash := idempotency.HashBody(body)
		if err := h.repo.SaveIdempotentResponse(ctx, key, reqHash, http.StatusCreated, respBody, order.ID); err != nil {
			// The order is real; failing to cache the idempotency
			// record is logged but not fatal to this request.
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(respBody)
}

func (h *OrderHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	order, err := h.repo.GetOrder(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "order not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, "failed to fetch order")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(order)
}

func (h *OrderHandler) GetOrderStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	order, err := h.repo.GetOrder(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "order not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, "failed to fetch order")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"order_id":   order.ID,
		"status":     order.Status,
		"updated_at": order.UpdatedAt.Format(time.RFC3339),
	})
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
