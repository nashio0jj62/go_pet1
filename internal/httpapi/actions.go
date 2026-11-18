package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strconv"
	"time"

	"payway/internal/payment"
	"payway/internal/provider"
)

func (a *API) cancelPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !payment.ValidID(id) {
		writeError(w, http.StatusBadRequest, "invalid payment ID")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	commandID, err := a.Payments.Cancel(ctx, id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeAccepted(w, id, commandID)
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
	events, err := a.Payments.History(ctx, id, after, limit)
	if err != nil {
		writeServiceError(w, err)
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
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	commandID, err := a.Payments.Callback(ctx, callback)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeAccepted(w, callback.PaymentID, commandID)
}
