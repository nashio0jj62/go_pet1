package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"payway/internal/payment"
	"payway/internal/telemetry"
)

type Request struct {
	PaymentID  string `json:"payment_id"`
	MerchantID string `json:"merchant_id"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
}

type Result struct {
	PaymentID     string         `json:"payment_id"`
	ProviderID    string         `json:"provider_id"`
	Status        payment.Status `json:"status"`
	FailureReason string         `json:"failure_reason,omitempty"`
	UpdatedAt     time.Time      `json:"updated_at"`
	TraceParent   string         `json:"-"`
}

type Callback struct {
	EventID string `json:"event_id"`
	Result
}

func (r Result) Validate() error {
	if !payment.ValidID(r.PaymentID) || r.ProviderID == "" || len(r.ProviderID) > 128 ||
		len(r.FailureReason) > 256 || r.UpdatedAt.IsZero() {
		return errors.New("invalid provider result")
	}
	if r.Status != payment.Processing && !r.Status.Terminal() {
		return errors.New("invalid provider status")
	}
	if r.Status != payment.Failed && r.FailureReason != "" {
		return errors.New("unexpected failure reason")
	}
	return nil
}

type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

func NewClient(baseURL, token string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"), Token: token,
		HTTP: &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *Client) Start(ctx context.Context, p payment.Payment) (Result, error) {
	return c.call(ctx, "/payments", p.ID, Request{
		PaymentID: p.ID, MerchantID: p.MerchantID, Amount: p.Amount, Currency: p.Currency,
	})
}

func (c *Client) Cancel(ctx context.Context, id string) (Result, error) {
	return c.call(ctx, "/payments/"+id+"/cancel", id, nil)
}

func (c *Client) call(ctx context.Context, path, id string, value any) (result Result, retErr error) {
	operation := "create"
	if value == nil {
		operation = "cancel"
	}
	ctx, span := telemetry.Tracer.Start(ctx, "provider."+operation, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("payment.id", id)))
	started := time.Now()
	defer func() {
		telemetry.ProviderRequest(ctx, operation, started)
		if retErr != nil {
			telemetry.Error(ctx, "provider", operation, retErr)
		}
		span.End()
	}()
	data, err := json.Marshal(value)
	if err != nil {
		return Result{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(data))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Idempotency-Key", id)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	response, err := c.HTTP.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer response.Body.Close()
	span.SetAttributes(attribute.Int("http.response.status_code", response.StatusCode))
	if response.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("provider returned HTTP %d", response.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 8192)).Decode(&result); err != nil {
		return Result{}, err
	}
	if result.PaymentID != id {
		return Result{}, errors.New("provider returned a different payment ID")
	}
	return result, result.Validate()
}
