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

	"payway/internal/payment"
	"payway/internal/service"
	"payway/internal/telemetry"
)

type API struct {
	Payments       *service.Payments
	CallbackSecret string
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", a.createPayment)
	mux.HandleFunc("GET /payments/{id}", a.getPayment)
	mux.HandleFunc("POST /payments/{id}/cancel", a.cancelPayment)
	mux.HandleFunc("GET /payments/{id}/events", a.paymentHistory)
	mux.HandleFunc("POST /callbacks/mock", a.providerCallback)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return telemetry.HTTP(mux)
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
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	id, err := a.Payments.Create(ctx, key, request)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeAccepted(w, id, "")
}

func (a *API) getPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !payment.ValidID(id) {
		writeError(w, http.StatusBadRequest, "invalid payment ID")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	p, err := a.Payments.Get(ctx, id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func writeAccepted(w http.ResponseWriter, paymentID, commandID string) {
	location := "/payments/" + paymentID
	w.Header().Set("Location", location)
	response := map[string]string{"payment_id": paymentID, "status_url": location}
	if commandID != "" {
		response["command_id"] = commandID
	}
	writeJSON(w, http.StatusAccepted, response)
}

func writeServiceError(w http.ResponseWriter, err error) {
	var failure *service.Error
	if !errors.As(err, &failure) {
		slog.Error("payment request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	status := http.StatusServiceUnavailable
	switch failure.Kind {
	case service.Invalid:
		status = http.StatusBadRequest
	case service.NotFound:
		status = http.StatusNotFound
	case service.Conflict:
		status = http.StatusConflict
	}
	if failure.Cause != nil {
		slog.Error(failure.Message, "error", failure.Cause)
	}
	writeError(w, status, failure.Message)
}

func invalidBody(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds 4096 bytes")
		return
	}
	writeError(w, http.StatusBadRequest, "body must contain one valid JSON object with the expected fields")
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
