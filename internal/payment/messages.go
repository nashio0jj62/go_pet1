package payment

import (
	"errors"
	"time"
)

const (
	CreatePayment     = "CreatePayment"
	CancelPayment     = "CancelPayment"
	ProviderCallback  = "ProviderCallback"
	PaymentCreated    = "PaymentCreated"
	PaymentProcessing = "PaymentProcessing"
	PaymentCompleted  = "PaymentCompleted"
	PaymentFailed     = "PaymentFailed"
	PaymentCanceled   = "PaymentCanceled"
)

type Command struct {
	ID            string        `json:"id"`
	Type          string        `json:"type"`
	SchemaVersion int           `json:"schema_version"`
	PaymentID     string        `json:"payment_id"`
	Data          CreateRequest `json:"data"`
	CreatedAt     time.Time     `json:"created_at"`
	ProviderID    string        `json:"provider_id,omitempty"`
	Result        Status        `json:"result,omitempty"`
	FailureReason string        `json:"failure_reason,omitempty"`
}

type Event struct {
	ID            string  `json:"id"`
	CommandID     string  `json:"command_id"`
	Type          string  `json:"type"`
	SchemaVersion int     `json:"schema_version"`
	Payment       Payment `json:"payment"`
}

func NewCommand(id string, data CreateRequest, createdAt time.Time) Command {
	return Command{
		ID: "create_" + id, Type: CreatePayment, SchemaVersion: 1,
		PaymentID: id, Data: data, CreatedAt: createdAt.UTC(),
	}
}

func CreatedEvent(c Command) (Event, error) {
	if c.Type != CreatePayment || c.SchemaVersion != 1 || !ValidID(c.PaymentID) ||
		c.ID != "create_"+c.PaymentID || c.CreatedAt.IsZero() {
		return Event{}, errors.New("invalid CreatePayment command")
	}
	if err := c.Data.Validate(); err != nil {
		return Event{}, err
	}
	return Event{
		ID: "created_" + c.PaymentID, CommandID: c.ID,
		Type: PaymentCreated, SchemaVersion: 1,
		Payment: Payment{
			ID: c.PaymentID, MerchantID: c.Data.MerchantID,
			Amount: c.Data.Amount, Currency: c.Data.Currency,
			Status: New, Version: 1,
			CreatedAt: c.CreatedAt, UpdatedAt: c.CreatedAt,
		},
	}, nil
}

func ChangeEvent(c Command, p Payment, status Status, providerID, reason string) (Event, error) {
	if err := ValidateTransition(p.Status, status); err != nil {
		return Event{}, err
	}
	p.Status = status
	p.Version++
	if c.CreatedAt.After(p.UpdatedAt) {
		p.UpdatedAt = c.CreatedAt.UTC()
	}
	p.ProviderID = providerID
	p.FailureReason = reason
	event := Event{
		ID: "event_" + c.ID, CommandID: c.ID,
		Type: eventType(status), SchemaVersion: 1, Payment: p,
	}
	return event, event.Validate()
}

func eventType(status Status) string {
	switch status {
	case New:
		return PaymentCreated
	case Processing:
		return PaymentProcessing
	case Completed:
		return PaymentCompleted
	case Failed:
		return PaymentFailed
	case Canceled:
		return PaymentCanceled
	default:
		return ""
	}
}

func (e Event) Validate() error {
	p := e.Payment
	if e.SchemaVersion != 1 || !ValidID(p.ID) || e.CommandID == "" || len(e.CommandID) > 200 ||
		e.Type == "" || e.Type != eventType(p.Status) || p.CreatedAt.IsZero() ||
		p.UpdatedAt.Before(p.CreatedAt) || len(p.ProviderID) > 128 || len(p.FailureReason) > 256 {
		return errors.New("invalid payment event")
	}
	if p.Status == New {
		if e.ID != "created_"+p.ID || e.CommandID != "create_"+p.ID || p.Version != 1 ||
			!p.CreatedAt.Equal(p.UpdatedAt) || p.ProviderID != "" || p.FailureReason != "" {
			return errors.New("invalid PaymentCreated event")
		}
	} else if e.ID != "event_"+e.CommandID || p.Version < 2 {
		return errors.New("invalid state change event")
	}
	if (p.Status == Processing || p.Status == Completed || p.Status == Failed) && p.ProviderID == "" {
		return errors.New("provider_id is required")
	}
	if p.Status != Failed && p.FailureReason != "" {
		return errors.New("failure_reason is only allowed for FAILED payments")
	}
	return (CreateRequest{MerchantID: p.MerchantID, Amount: p.Amount, Currency: p.Currency}).Validate()
}
