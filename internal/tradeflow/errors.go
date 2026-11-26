package tradeflow

import (
	"commons/apierror"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Codes for operator-facing trade-start failures. The dashboard branches on these.
const (
	CodeInvalidRoute      = "invalid_route"
	CodeUnsupportedRoute  = "unsupported_route"
	CodeNoHedgeVenue      = "no_hedge_venue"
	CodePreTradeFailed    = "pre_trade_checks_failed"
	CodeSpotBuyFailed     = "spot_buy_failed"
	CodeHedgeFailed       = "hedge_failed"
	CodeWithdrawalFailed  = "withdrawal_failed"
	CodeBookkeepingFailed = "bookkeeping_failed"
	CodeNeedsReconcile    = "needs_reconciliation"
	CodeTradeflowOff      = "tradeflow_unavailable"
)

// Trade legs, used by the dashboard to show which part of a trade failed.
const (
	LegSpot       = "spot"
	LegHedge      = "hedge"
	LegWithdrawal = "withdrawal"
	LegDEX        = "dex"
)

// RouteProblem reports a trade request the dashboard should never send. The
// operator can only retry with a fresh route, so the message says that and the
// field-level reason is kept as technical detail.
func RouteProblem(format string, args ...any) error {
	return apierror.Wrap(fmt.Errorf(format, args...), CodeInvalidRoute,
		"This trade's route is incomplete or inconsistent, so nothing was started. Close the confirmation and reopen the offer to get a fresh route; if it keeps happening, report it with the technical details.").
		WithStatus(http.StatusBadRequest)
}

// ErrMostroRoute explains Mostro's bid-over-Lightning restriction.
func ErrMostroRoute() error {
	return apierror.New(CodeUnsupportedRoute, "Mostro offers can only be taken as a bid paid over Lightning. Enable the Swap route in the liquidity target, or pick another offer.").
		WithStatus(http.StatusBadRequest)
}

// ErrHedgelessAskSide explains that hedgeless mode only takes bids.
func ErrHedgelessAskSide() error {
	return apierror.New(CodeUnsupportedRoute, "Hedgeless mode can only take offers where the maker buys bitcoin (bids). Pick a bid offer, or switch the liquidity target to hedgefull mode.").
		WithStatus(http.StatusBadRequest)
}

// ErrHedgelessDEX explains that hedgeless mode does not support a venue.
func ErrHedgelessDEX(dex string) error {
	return apierror.Newf(CodeUnsupportedRoute, "Hedgeless mode can't take %s offers; it supports Bisq, HodlHodl and RoboSats. Switch the liquidity target to hedgefull mode to take this offer.", displayVenue(dex)).
		WithStatus(http.StatusBadRequest)
}

// ErrHedgelessProvider explains that hedgeless mode cannot refill the pool from an exchange.
func ErrHedgelessProvider(provider string, supported ...string) error {
	return apierror.Newf(CodeUnsupportedRoute, "Hedgeless mode can't refill the liquidity pool from %s; it supports %s. Pick one of those exchanges for this trade.", displayVenue(provider), strings.Join(supported, " and ")).
		WithStatus(http.StatusBadRequest)
}

// ErrNoHedgeVenue explains a hedgefull route without a hedge.
func ErrNoHedgeVenue() error {
	return apierror.New(CodeNoHedgeVenue, "Hedgefull mode needs a futures exchange to hedge this trade, but none is available. Connect an exchange with hedging enabled under Trading Accounts, or switch the liquidity target to hedgeless mode.").
		WithStatus(http.StatusConflict)
}

func displayVenue(name string) string {
	name = strings.TrimSpace(name)
	switch strings.ToLower(name) {
	case "":
		return "this venue's"
	case "hodlhodl":
		return "HodlHodl"
	case "robosats":
		return "RoboSats"
	case "bisq":
		return "Bisq"
	case "mostro":
		return "Mostro"
	case "peach":
		return "Peach"
	}
	return strings.ToUpper(name[:1]) + name[1:]
}

// explain returns the operator message of err when it has one, else its text.
func explain(err error) string {
	if ae, ok := apierror.As(err); ok {
		return ae.Message
	}
	return err.Error()
}

// spotBuyFailed words a failed market buy. A timeout or lost connection may
// still have placed the order, so then the operator must check before retrying.
func spotBuyFailed(provider string, technical, cause error) error {
	if errors.Is(cause, context.DeadlineExceeded) || apierror.IsUnreachable(cause) {
		return apierror.Wrapf(technical, CodeSpotBuyFailed,
			"%s didn't answer the spot buy in time, so the order may or may not have been placed. Check %s's order history before retrying. Reason: %s", provider, provider, explain(cause)).
			WithStatus(http.StatusGatewayTimeout).WithLeg(LegSpot)
	}
	return apierror.Wrapf(technical, CodeSpotBuyFailed,
		"Buying BTC on %s failed, so no hedge or withdrawal was started: %s", provider, explain(cause)).
		WithStatus(http.StatusBadGateway).WithLeg(LegSpot)
}
