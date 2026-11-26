package robosats

import (
	"commons/apierror"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"
)

func testPosition(t *testing.T, overrides string) *Position {
	t.Helper()
	base := map[string]any{"id": 42, "status": 1, "type": 0, "currency": 2, "has_range": true, "min_amount": "50", "max_amount": "200",
		"payment_method": "SEPA", "price_now": "100000", "bond_size": "3", "expires_at": "2030-01-01T00:00:00Z"}
	if overrides != "" {
		var extra map[string]any
		if err := json.Unmarshal([]byte(overrides), &extra); err != nil {
			t.Fatal(err)
		}
		for k, v := range extra {
			base[k] = v
		}
	}
	raw, _ := json.Marshal(base)
	var p Position
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	return &p
}

func quoteErr(t *testing.T, p *Position, in QuoteInput) *apierror.Error {
	t.Helper()
	if in.OrderID == "" {
		in = QuoteInput{OrderID: "42", CoordinatorID: "coord", Amount: "100", Unit: "fiat", Currency: "EUR", PaymentMethod: "SEPA"}
	}
	_, err := quoteFromPosition(p, in, "0.001", "node", time.Now())
	if err == nil {
		t.Fatal("quote unexpectedly succeeded")
	}
	ae, ok := apierror.As(err)
	if !ok {
		t.Fatalf("error has no operator message: %v", err)
	}
	return ae
}

func TestQuoteExplainsWhyAnOfferCannotBeTaken(t *testing.T) {
	cases := []struct {
		overrides, code, contains string
	}{
		{`{"status":2}`, CodeOfferUnavailable, "its maker paused it"},
		{`{"status":3}`, CodeOfferUnavailable, "another robot is taking it right now"},
		{`{"status":9}`, CodeOfferUnavailable, "another robot already took it"},
		{`{"type":1}`, CodeOfferNotTakeable, "sells bitcoin"},
		{`{"is_maker":true}`, CodeOfferNotTakeable, "your own robot"},
		{`{"has_password":true}`, CodeOfferPassword, "password-protected"},
		{`{"currency":1}`, CodeCurrencyChanged, "no longer priced in EUR on RoboSats (it now uses USD)"},
		{`{"payment_method":"Revolut"}`, CodePaymentMethod, `which lists "Revolut"`},
		{`{"expires_at":"2000-01-01T00:00:00Z"}`, CodeOfferExpiring, "expires in less than 30 seconds"},
	}
	for _, tc := range cases {
		ae := quoteErr(t, testPosition(t, tc.overrides), QuoteInput{})
		if ae.Code != tc.code || !strings.Contains(ae.Message, tc.contains) {
			t.Errorf("%s: got %q %q, want code %q containing %q", tc.overrides, ae.Code, ae.Message, tc.code, tc.contains)
		}
	}
}

func TestQuoteNamesTheOfferLimits(t *testing.T) {
	ae := quoteErr(t, testPosition(t, ""), QuoteInput{OrderID: "42", Amount: "500", Unit: "fiat", Currency: "EUR", PaymentMethod: "SEPA"})
	if ae.Code != CodeAmountOutOfRange || ae.Message != "500.00 EUR is outside offer #42's limits of 50–200 EUR. Enter an amount within that range." {
		t.Fatalf("unexpected limit message: %q %q", ae.Code, ae.Message)
	}
	fixed := testPosition(t, `{"has_range":false,"amount":"75"}`)
	ae = quoteErr(t, fixed, QuoteInput{OrderID: "42", Amount: "100", Unit: "fiat", Currency: "EUR", PaymentMethod: "SEPA"})
	if !strings.Contains(ae.Message, "is for exactly 75 EUR") {
		t.Fatalf("fixed-amount message: %q", ae.Message)
	}
}

func TestCoordinatorErrorsSayWhatToDo(t *testing.T) {
	forbidden := &ProtocolError{Message: "robosats: GET /api/robot/ returned HTTP 403", Status: 403}
	if ae, _ := apierror.As(explainCoordinatorError(forbidden, "load your robot")); ae == nil || ae.Code != CodeRobotRejected || !strings.Contains(ae.Message, "Trading Accounts") {
		t.Fatalf("403 on robot: %+v", ae)
	}
	gone := &ProtocolError{Message: "robosats: GET /api/order/ returned HTTP 404", Status: 404}
	if ae, _ := apierror.As(explainCoordinatorError(gone, "load this offer")); ae == nil || ae.Code != CodeOfferUnavailable {
		t.Fatalf("404 on offer: %+v", ae)
	}
	refused := &ProtocolError{Message: "robosats: POST returned HTTP 400 (This order has been taken)", Status: 400, Detail: "This order has been taken"}
	if ae, _ := apierror.As(explainCoordinatorError(refused, "load this offer")); ae == nil || ae.Message != "The RoboSats coordinator refused to load this offer: This order has been taken." {
		t.Fatalf("400 with reason: %+v", ae)
	}
	tor := fmt.Errorf("robosats: GET /api/robot/ failed: %w", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED})
	if ae, _ := apierror.As(explainCoordinatorError(tor, "load your robot")); ae == nil || ae.Code != CodeCoordinatorUnreachable || !strings.Contains(ae.Message, "over Tor") {
		t.Fatalf("unreachable: %+v", ae)
	}
	timeout := fmt.Errorf("robosats: GET /api/info/ failed: %w", context.DeadlineExceeded)
	if ae, _ := apierror.As(explainCoordinatorError(timeout, "load its fee and node information")); ae == nil || ae.Code != CodeCoordinatorUnreachable {
		t.Fatalf("timeout: %+v", ae)
	}
}

func TestRejectedStatusKeepsTheCoordinatorsReason(t *testing.T) {
	status, detail := RejectedStatus(fmt.Errorf("take: %w", &ProtocolError{Message: "x", Status: 400, Detail: "Invalid amount"}))
	if status != 400 || detail != "Invalid amount" {
		t.Fatalf("got %d %q", status, detail)
	}
	if status, _ := RejectedStatus(fmt.Errorf("plain")); status != 0 {
		t.Fatalf("plain error reported status %d", status)
	}
}

func TestClientKeepsTheReasonOfARefusedRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"bad_request":"This order has been cancelled by the maker"}`))
	}))
	defer srv.Close()
	_, err := NewClient(Config{BaseURL: srv.URL}).GetPosition(context.Background(), "42")
	status, detail := RejectedStatus(err)
	if status != http.StatusBadRequest || detail != "This order has been cancelled by the maker" || !IsPermanent(err) {
		t.Fatalf("got status %d detail %q permanent %t: %v", status, detail, IsPermanent(err), err)
	}
	if !strings.Contains(err.Error(), "(This order has been cancelled by the maker)") {
		t.Fatalf("error text lost the reason: %v", err)
	}
}
