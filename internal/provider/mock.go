package provider

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"payway/internal/payment"
)

type Mock struct {
	DB          *pgxpool.Pool
	Secret      string
	CallbackURL string
	HTTP        *http.Client
}

func (m *Mock) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", m.start)
	mux.HandleFunc("POST /payments/{id}/cancel", m.cancel)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.Secret == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")),
			[]byte("Bearer "+m.Secret)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (m *Mock) start(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input Request
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "expected one JSON object", http.StatusBadRequest)
		return
	}
	data := payment.CreateRequest{MerchantID: input.MerchantID, Amount: input.Amount, Currency: input.Currency}
	if !payment.ValidID(input.PaymentID) || data.Validate() != nil || r.Header.Get("Idempotency-Key") != input.PaymentID {
		http.Error(w, "invalid payment", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, err := m.DB.Exec(ctx, `INSERT INTO mock_provider_payments
		(payment_id, merchant_id, amount, currency, status) VALUES ($1, $2, $3, $4, 'PROCESSING')
		ON CONFLICT (payment_id) DO NOTHING`, input.PaymentID, input.MerchantID, input.Amount, input.Currency)
	if m.databaseError(w, err) {
		return
	}
	var stored Request
	err = m.DB.QueryRow(ctx, `SELECT merchant_id, amount, currency FROM mock_provider_payments WHERE payment_id = $1`,
		input.PaymentID).Scan(&stored.MerchantID, &stored.Amount, &stored.Currency)
	if m.databaseError(w, err) {
		return
	}
	if stored.MerchantID != input.MerchantID || stored.Amount != input.Amount || stored.Currency != input.Currency {
		http.Error(w, "payment ID reused with different data", http.StatusConflict)
		return
	}
	result, err := m.result(ctx, input.PaymentID)
	if !m.databaseError(w, err) {
		mockJSON(w, result)
	}
}

func (m *Mock) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !payment.ValidID(id) || r.Header.Get("Idempotency-Key") != id {
		http.Error(w, "invalid payment ID", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, err := m.DB.Exec(ctx, `UPDATE mock_provider_payments SET status = 'CANCELED', updated_at = now(),
		next_attempt_at = now() WHERE payment_id = $1 AND status = 'PROCESSING'`, id)
	if m.databaseError(w, err) {
		return
	}
	result, err := m.result(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "payment not found", http.StatusNotFound)
		return
	}
	if !m.databaseError(w, err) {
		mockJSON(w, result)
	}
}

func (m *Mock) result(ctx context.Context, id string) (Result, error) {
	var result Result
	err := m.DB.QueryRow(ctx, `SELECT payment_id, status, failure_reason, updated_at
		FROM mock_provider_payments WHERE payment_id = $1`, id).Scan(
		&result.PaymentID, &result.Status, &result.FailureReason, &result.UpdatedAt)
	result.ProviderID = "mock_" + id
	result.UpdatedAt = result.UpdatedAt.UTC()
	return result, err
}

func (m *Mock) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.deliver(ctx); err != nil && ctx.Err() == nil {
				slog.Error("mock provider delivery", "error", err)
			}
		}
	}
}

func (m *Mock) deliver(ctx context.Context) error {
	dbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_, err := m.DB.Exec(dbCtx, `UPDATE mock_provider_payments SET
		status = CASE WHEN amount % 100 = 13 THEN 'FAILED' ELSE 'COMPLETED' END,
		failure_reason = CASE WHEN amount % 100 = 13 THEN 'mock_declined' ELSE '' END,
		updated_at = now(), next_attempt_at = now()
		WHERE status = 'PROCESSING' AND ready_at <= now() AND amount % 100 <> 99`)
	cancel()
	if err != nil {
		return err
	}
	dbCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	rows, err := m.DB.Query(dbCtx, `SELECT payment_id FROM mock_provider_payments
		WHERE status <> 'PROCESSING' AND callback_sent_at IS NULL AND next_attempt_at <= now()
		ORDER BY next_attempt_at LIMIT 20`)
	if err != nil {
		cancel()
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			cancel()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	cancel()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		dbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err := m.result(dbCtx, id)
		cancel()
		if err != nil {
			return err
		}
		callback := Callback{EventID: "mock_" + id + "_" + string(result.Status), Result: result}
		sent := m.send(ctx, callback)
		dbCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		_, err = m.DB.Exec(dbCtx, `UPDATE mock_provider_payments SET
			callback_sent_at = CASE WHEN $2 THEN now() ELSE NULL END,
			next_attempt_at = now() + interval '5 seconds' WHERE payment_id = $1`, id, sent)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Mock) send(ctx context.Context, callback Callback) bool {
	body, err := json.Marshal(callback)
	if err != nil {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.CallbackURL, bytes.NewReader(body))
	if err != nil {
		slog.Error("create callback request", "error", err)
		return false
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Callback-Timestamp", timestamp)
	req.Header.Set("X-Callback-Signature", Signature(m.Secret, timestamp, body))
	response, err := m.HTTP.Do(req)
	if err != nil {
		slog.Warn("callback delivery failed", "payment_id", callback.PaymentID, "error", err)
		return false
	}
	defer response.Body.Close()
	io.Copy(io.Discard, io.LimitReader(response.Body, 8192))
	if response.StatusCode != http.StatusAccepted {
		slog.Warn("callback rejected", "payment_id", callback.PaymentID, "status", response.StatusCode)
		return false
	}
	return true
}

func (m *Mock) databaseError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	slog.Error("mock provider database", "error", err)
	http.Error(w, "provider storage unavailable", http.StatusServiceUnavailable)
	return true
}

func mockJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Warn("write provider response", "error", err)
	}
}
