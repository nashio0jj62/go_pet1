package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"time"

	"payway/internal/broker"
	"payway/internal/payment"
	"payway/internal/provider"
	"payway/internal/storage"
)

func (a *API) cancelPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !payment.ValidID(id) {
		writeError(w, http.StatusBadRequest, "invalid payment ID")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	p, err := a.Store.Get(ctx, id)
	if readError(w, err) {
		return
	}
	if p.Status == payment.Completed || p.Status == payment.Failed {
		writeError(w, http.StatusConflict, "payment is already finished")
		return
	}
	commandID := "cancel_" + id
	createdAt, err := a.Store.ReserveRequest(ctx, commandID, commandID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "request storage unavailable")
		return
	}
	command := payment.Command{
		ID: commandID, Type: payment.CancelPayment, SchemaVersion: 1,
		PaymentID: id, CreatedAt: createdAt,
	}
	a.accept(w, ctx, command)
}

func (a *API) paymentHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !payment.ValidID(id) {
		writeError(w, http.StatusBadRequest, "invalid payment ID")
		return
	}
	after, limit := 0, 50
	var err error
	if raw := r.URL.Query().Get("after_version"); raw != "" {
		after, err = strconv.Atoi(raw)
		if err != nil || after < 0 || after > 2147483647 {
			writeError(w, http.StatusBadRequest, "invalid after_version")
			return
		}
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if _, err := a.Store.Get(ctx, id); readError(w, err) {
		return
	}
	events, err := a.Store.History(ctx, id, after, limit)
	if readError(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (a *API) providerCallback(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		invalidBody(w, err)
		return
	}
	if !provider.Verify(a.CallbackSecret, r.Header.Get("X-Callback-Timestamp"),
		r.Header.Get("X-Callback-Signature"), body, time.Now()) {
		writeError(w, http.StatusUnauthorized, "invalid callback signature or timestamp")
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var callback provider.Callback
	if err := decoder.Decode(&callback); err != nil {
		invalidBody(w, err)
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		invalidBody(w, err)
		return
	}
	if err := callback.Result.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if payment.ValidateKey(callback.EventID) != nil || !callback.Status.Terminal() {
		writeError(w, http.StatusBadRequest, "invalid callback event ID or status")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	p, err := a.Store.Get(ctx, callback.PaymentID)
	if readError(w, err) {
		return
	}
	if p.ProviderID != "" && p.ProviderID != callback.ProviderID {
		writeError(w, http.StatusConflict, "provider reference does not match payment")
		return
	}
	idSum := sha256.Sum256([]byte(callback.EventID))
	commandID := "callback_" + hex.EncodeToString(idSum[:])
	canonical, _ := json.Marshal(callback)
	bodySum := sha256.Sum256(canonical)
	_, err = a.Store.ReserveRequest(ctx, commandID, hex.EncodeToString(bodySum[:]))
	if errors.Is(err, storage.ErrConflict) {
		writeError(w, http.StatusConflict, "callback event ID reused with different data")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "request storage unavailable")
		return
	}
	a.accept(w, ctx, payment.Command{
		ID: commandID, Type: payment.ProviderCallback, SchemaVersion: 1,
		PaymentID: callback.PaymentID, CreatedAt: callback.UpdatedAt.UTC(),
		ProviderID: callback.ProviderID, Result: callback.Status, FailureReason: callback.FailureReason,
	})
}

func (a *API) accept(w http.ResponseWriter, ctx context.Context, command payment.Command) {
	if err := broker.Publish(ctx, a.Commands, command.PaymentID, command); err != nil {
		slog.Error("publish command", "command_id", command.ID, "error", err)
		writeError(w, http.StatusServiceUnavailable, "publication not confirmed; retry the same request")
		return
	}
	w.Header().Set("Location", "/payments/"+command.PaymentID)
	writeJSON(w, http.StatusAccepted, map[string]string{
		"payment_id": command.PaymentID, "command_id": command.ID,
		"status_url": "/payments/" + command.PaymentID,
	})
}

func readError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "payment not found or not yet projected")
	} else {
		slog.Error("read payment data", "error", err)
		writeError(w, http.StatusServiceUnavailable, "payment storage unavailable")
	}
	return true
}
