package payment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

type Status string

const (
	New        Status = "NEW"
	Processing Status = "PROCESSING"
	Completed  Status = "COMPLETED"
	Failed     Status = "FAILED"
	Canceled   Status = "CANCELED"
)

var (
	merchantPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
	idPattern       = regexp.MustCompile(`^pay_[a-f0-9]{64}$`)
)

type CreateRequest struct {
	MerchantID string `json:"merchant_id"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
}

func (r CreateRequest) Validate() error {
	if !merchantPattern.MatchString(r.MerchantID) {
		return errors.New("merchant_id must contain 1-64 letters, digits, underscores or hyphens")
	}
	if r.Amount <= 0 {
		return errors.New("amount must be a positive integer in minor currency units")
	}
	switch r.Currency {
	case "KZT", "RUB", "USD", "EUR":
	default:
		return errors.New("currency must be one of KZT, RUB, USD, EUR")
	}
	return nil
}

func ValidateKey(key string) error {
	if len(key) < 1 || len(key) > 128 {
		return errors.New("Idempotency-Key must contain 1-128 printable ASCII characters")
	}
	for _, c := range key {
		if c < 33 || c > 126 {
			return errors.New("Idempotency-Key must contain 1-128 printable ASCII characters")
		}
	}
	return nil
}

func ID(merchantID, key string) string {
	sum := sha256.Sum256([]byte(merchantID + "\x00" + key))
	return "pay_" + hex.EncodeToString(sum[:])
}

func ValidID(id string) bool {
	return idPattern.MatchString(id)
}

func (r CreateRequest) Fingerprint() string {
	data, _ := json.Marshal(r) // This struct contains only strings and an integer.
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func ValidateTransition(from, to Status) error {
	allowed := false
	switch from {
	case "":
		allowed = to == New
	case New:
		allowed = to == Processing || to == Canceled
	case Processing:
		allowed = to == Completed || to == Failed || to == Canceled
	}
	if !allowed {
		return fmt.Errorf("invalid payment transition: %q -> %q", from, to)
	}
	return nil
}

type Payment struct {
	ID         string    `json:"id"`
	MerchantID string    `json:"merchant_id"`
	Amount     int64     `json:"amount"`
	Currency   string    `json:"currency"`
	Status     Status    `json:"status"`
	Version    int       `json:"version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

