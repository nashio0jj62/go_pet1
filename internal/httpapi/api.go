package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"

	"payway/internal/broker"
	"payway/internal/payment"
	"payway/internal/storage"
)

type API struct {
	Store    *storage.Store
	Commands *kafka.Writer
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", a.createPayment)
	mux.HandleFunc("GET /payments/{id}", a.getPayment)
	// Liveness only: this does not promise that Kafka or PostgreSQL is available.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func (a *API) createPayment(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if err := payment.ValidateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request payment.CreateRequest
	if err := decoder.Decode(&request); err != nil {
		invalidBody(w, err)
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		invalidBody(w, err)
		return
	}
	if err := request.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	id := payment.ID(request.MerchantID, key)
	createdAt, err := a.Store.Reserve(ctx, id, request)
	if errors.Is(err, storage.ErrConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		slog.Error("reserve payment request", "payment_id", id, "error", err)
		writeError(w, http.StatusServiceUnavailable, "request storage unavailable; retry with the same Idempotency-Key")
		return
	}
	command := payment.NewCommand(id, request, createdAt)
	if err := broker.Publish(ctx, a.Commands, id, command); err != nil {
		slog.Error("publish payment command", "payment_id", id, "error", err)
		writeError(w, http.StatusServiceUnavailable, "publication not confirmed; retry with the same Idempotency-Key")
		return
	}
	slog.Info("payment command accepted", "payment_id", id, "command_id", command.ID)
	w.Header().Set("Location", "/payments/"+id)
	writeJSON(w, http.StatusAccepted, map[string]string{
		"payment_id": id, "status_url": "/payments/" + id,
	})
}

func (a *API) getPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !payment.ValidID(id) {
		writeError(w, http.StatusBadRequest, "invalid payment ID")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	p, err := a.Store.Get(ctx, id)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "payment not found or not yet projected")
		return
	}
	if err != nil {
		slog.Error("read payment", "payment_id", id, "error", err)
		writeError(w, http.StatusServiceUnavailable, "payment storage unavailable")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func invalidBody(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds 4096 bytes")
		return
	}
	writeError(w, http.StatusBadRequest, "body must contain one valid JSON object with merchant_id, amount and currency")
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Warn("write HTTP response", "error", err)
	}
}

