package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"payway/internal/broker"
	"payway/internal/payment"
)

func TestCommandOutbox(t *testing.T) {
	store := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	request := payment.CreateRequest{MerchantID: "demo", Amount: 12500, Currency: "KZT"}
	id := payment.ID(request.MerchantID, "outbox-test")
	command := payment.NewCommand(id, request, time.Time{})
	const trace = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"

	if _, err := store.EnqueueCommand(ctx, id, request.Fingerprint(), trace, command); err != nil {
		t.Fatal(err)
	}
	if parent, err := store.EnqueueCommand(ctx, id, request.Fingerprint(), "", command); err != nil || parent != trace {
		t.Fatalf("repeat returned parent %q, error %v", parent, err)
	}
	if _, err := store.EnqueueCommand(ctx, id, "different", trace, command); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed request returned %v", err)
	}
	if _, err := store.EnqueueCommand(ctx, "other-request", request.Fingerprint(), trace, command); err == nil {
		t.Fatal("expected outbox command ID conflict")
	}
	if parent, err := store.TraceParent(ctx, "other-request"); err != nil || parent != "" {
		t.Fatalf("failed enqueue left a reservation: %q, %v", parent, err)
	}

	brokerDown := errors.New("broker unavailable")
	var first OutboxCommand
	sent, err := store.DispatchCommand(ctx, func(_ context.Context, item OutboxCommand) error {
		first = item
		return brokerDown
	})
	if sent || !errors.Is(err, brokerDown) {
		t.Fatalf("failed publish reported sent=%v, err=%v", sent, err)
	}
	var retried OutboxCommand
	sent, err = store.DispatchCommand(ctx, func(_ context.Context, item OutboxCommand) error {
		retried = item
		return nil
	})
	if !sent || err != nil {
		t.Fatalf("retry reported sent=%v, err=%v", sent, err)
	}
	if retried.ID != command.ID || retried.PaymentID != id || retried.TraceParent != trace || !bytes.Equal(first.Payload, retried.Payload) {
		t.Fatalf("unexpected retried command: %+v", retried)
	}
	var decoded payment.Command
	if err := json.Unmarshal(retried.Payload, &decoded); err != nil || decoded.CreatedAt.IsZero() || decoded.Data != request {
		t.Fatalf("invalid outbox payload: %+v, %v", decoded, err)
	}
	if _, err := store.EnqueueCommand(ctx, id, request.Fingerprint(), trace, command); err != nil {
		t.Fatal(err)
	}
	sent, err = store.DispatchCommand(ctx, func(context.Context, OutboxCommand) error {
		t.Error("a completed publication was enqueued again")
		return nil
	})
	if sent || err != nil {
		t.Fatalf("empty outbox reported sent=%v, err=%v", sent, err)
	}

	legacyID := payment.ID(request.MerchantID, "legacy-test")
	legacyTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	if _, err := store.DB.Exec(ctx, `INSERT INTO payment_requests (payment_id, fingerprint, created_at)
		VALUES ($1, $2, $3)`, legacyID, request.Fingerprint(), legacyTime); err != nil {
		t.Fatal(err)
	}
	legacy := payment.NewCommand(legacyID, request, time.Time{})
	if _, err := store.EnqueueCommand(ctx, legacyID, request.Fingerprint(), trace, legacy); err != nil {
		t.Fatal(err)
	}
	sent, err = store.DispatchCommand(ctx, func(_ context.Context, item OutboxCommand) error {
		var decoded payment.Command
		if err := json.Unmarshal(item.Payload, &decoded); err != nil {
			return err
		}
		if decoded.ID != legacy.ID || !decoded.CreatedAt.Equal(legacyTime) {
			t.Errorf("retry changed legacy command: %+v", decoded)
		}
		return nil
	})
	if !sent || err != nil {
		t.Fatalf("legacy request dispatch reported sent=%v, err=%v", sent, err)
	}
}

func TestRejectedMessageIsSavedOnce(t *testing.T) {
	store := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rejection := broker.RejectedMessage{
		Group: "payment-workers", Topic: "payment.commands", Partition: 2, Offset: 4,
		Key: []byte("payment"), Payload: []byte{0xff, '{'}, Reason: "invalid JSON",
	}
	for i := 0; i < 2; i++ {
		if err := store.SaveRejection(ctx, rejection); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := store.DB.QueryRow(ctx, `SELECT count(*) FROM rejected_messages`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("got %d rejections, error %v", count, err)
	}
	var payload []byte
	var reason string
	if err := store.DB.QueryRow(ctx, `SELECT payload, reason FROM rejected_messages`).Scan(&payload, &reason); err != nil ||
		!bytes.Equal(payload, rejection.Payload) || reason != rejection.Reason {
		t.Fatalf("rejection changed when saved: %x, %q, %v", payload, reason, err)
	}
}

func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("PAYWAY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PAYWAY_TEST_DATABASE_URL is required for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := fmt.Sprintf("payway_test_%x", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	testConfig := config.Copy()
	testConfig.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, testConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	files, err := filepath.Glob("../../migrations/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("migration files: %v (%v)", files, err)
	}
	for _, file := range files {
		sql, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", file, err)
		}
	}
	return &Store{DB: db}
}
