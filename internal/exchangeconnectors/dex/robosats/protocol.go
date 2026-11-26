package robosats

import (
	"errors"
	"fmt"
	"strings"
)

// Permanent errors must never be treated as a transient transport outage.
// Status and Detail are set when the coordinator refused a request with a 4xx
// answer: Detail is the explanation it gave (its "bad_request" text), if any.
type ProtocolError struct {
	Message string
	Status  int
	Detail  string
}

func (e *ProtocolError) Error() string { return e.Message }
func permanent(format string, args ...any) error {
	return &ProtocolError{Message: fmt.Sprintf(format, args...)}
}
func IsPermanent(err error) bool { var p *ProtocolError; return errors.As(err, &p) }

// RejectedStatus returns the coordinator's 4xx status and explanation when err
// is a refused request, or 0 otherwise.
func RejectedStatus(err error) (int, string) {
	var p *ProtocolError
	if errors.As(err, &p) {
		return p.Status, p.Detail
	}
	return 0, ""
}

// Coordinator order status codes from the upstream Order model.
const (
	StatusWaitingMakerBond      = 0
	StatusPublic                = 1
	StatusPaused                = 2
	StatusWaitingTakerBond      = 3
	StatusCancelled             = 4
	StatusExpired               = 5
	StatusWaitingBuyerInvoice   = 6
	StatusWaitingSellerEscrow   = 7
	StatusWaitingBothContracts  = 8
	StatusSendingFiat           = 9
	StatusFiatSent              = 10
	StatusInDispute             = 11
	StatusCollaborativeCancel   = 12
	StatusSendingSats           = 13
	StatusSuccessfulTrade       = 14
	StatusFailedLightning       = 15
	StatusWaitDisputeResolution = 16
	StatusMakerLostDispute      = 17
	StatusTakerLostDispute      = 18
)

// Order type: 0 = maker buys BTC, 1 = maker sells BTC.
const (
	TypeBuy  = 0
	TypeSell = 1
)

// Durable execution phases shared by hedgefull and hedgeless workers.
const (
	PhaseCreated         = "created"
	PhaseTaking          = "taking"
	PhaseTaken           = "taken"
	PhaseBondSubmitted   = "bond_submitted"
	PhaseBondLocked      = "bond_locked"
	PhaseEscrowSubmitted = "escrow_submitted"
	PhaseEscrowLocked    = "escrow_locked"
	PhaseChatReady       = "chat_ready"
	PhaseChatSent        = "chat_sent"
	PhaseNeedsReview     = "needs_review"
)

// ValidateResponse rejects coordinator error payloads even when HTTP is 200.
func (p *Position) ValidateResponse() error {
	if p == nil {
		return permanent("robosats: empty order response")
	}
	if msg := strings.TrimSpace(p.BadRequest); msg != "" {
		return permanent("robosats: %s", msg)
	}
	if p.Status < StatusWaitingMakerBond || p.Status > StatusTakerLostDispute {
		return permanent("robosats: unknown order status %d", p.Status)
	}
	switch p.Status {
	case StatusCancelled, StatusExpired, StatusCollaborativeCancel,
		StatusFailedLightning, StatusMakerLostDispute, StatusTakerLostDispute:
		if p.StatusMessage != "" {
			return permanent("robosats: order %s is terminal (%s)", p.ID, p.StatusMessage)
		}
		return permanent("robosats: order %s is in terminal status %d", p.ID, p.Status)
	}
	return nil
}

// ChatReady reports whether the coordinator has locked seller escrow and opened chat.
func (p *Position) ChatReady() bool {
	if p == nil {
		return false
	}
	return p.EscrowLocked && (p.Status == StatusSendingFiat || p.Status == StatusFiatSent || p.ChatLastIndex >= 0 && p.Status >= StatusSendingFiat)
}

// BondLocked reports coordinator-confirmed taker (or maker) bond lock.
func (p *Position) BondLocked() bool {
	if p == nil {
		return false
	}
	if p.IsTaker {
		return p.TakerLocked
	}
	if p.IsMaker {
		return p.MakerLocked
	}
	return p.TakerLocked || p.MakerLocked
}
