package payment

import (
	"errors"
	"time"
)

const (
	CreatePayment  = "CreatePayment"
	PaymentCreated = "PaymentCreated"
)

type Command struct {
	ID            string        `json:"id"`
	Type          string        `json:"type"`
	SchemaVersion int           `json:"schema_version"`
	PaymentID     string        `json:"payment_id"`
	Data          CreateRequest `json:"data"`
	CreatedAt     time.Time     `json:"created_at"`
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

// A redelivered command produces exactly the same event, including its timestamps.
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

func (e Event) Validate() error {
	p := e.Payment
	if e.Type != PaymentCreated || e.SchemaVersion != 1 || !ValidID(p.ID) ||
		e.ID != "created_"+p.ID || e.CommandID != "create_"+p.ID || p.Version != 1 ||
		p.CreatedAt.IsZero() || !p.UpdatedAt.Equal(p.CreatedAt) {
		return errors.New("invalid PaymentCreated event")
	}
	if err := ValidateTransition("", p.Status); err != nil {
		return err
	}
	return (CreateRequest{MerchantID: p.MerchantID, Amount: p.Amount, Currency: p.Currency}).Validate()
}

