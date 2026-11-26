package bisqexec

import (
	"commons/apierror"
	"fmt"
	"net/http"
)

// Codes for operator-facing Bisq execution failures. The dashboard branches on these.
const (
	CodeBisqDisabled        = "bisq_disabled"
	CodeBisqSetup           = "bisq_setup_incomplete"
	CodeBisqQuoteMismatch   = "bisq_quote_mismatch"
	CodeBisqLimitExceeded   = "bisq_limit_exceeded"
	CodeQuoteExpired        = "quote_expired"
	CodeExecutionExpired    = "bisq_execution_expired"
	CodeIdempotencyConflict = "idempotency_conflict"
	CodeExecutionConflict   = "bisq_execution_conflict"
	CodeQuoteNotFound       = "quote_not_found"
)

// setupProblem keeps ErrAutomatedDisabled in the chain (callers test for it)
// while telling the operator what to configure.
func setupProblem(technical string, format string, args ...any) error {
	return apierror.Wrapf(fmt.Errorf("%w: %s", ErrAutomatedDisabled, technical), CodeBisqSetup, format, args...).
		WithStatus(http.StatusServiceUnavailable)
}

// quoteMismatch reports a daemon or route answer that does not match the request.
func quoteMismatch(technical error) error {
	return apierror.Wrap(technical, CodeBisqQuoteMismatch,
		"The Bisq daemon's quote doesn't match the requested trade (the offer may have changed or been taken), so nothing was started. Refresh the offer and request a new Bisq quote.").
		WithStatus(http.StatusConflict)
}

// limitExceeded reports a quote outside the operator's spending limits.
func limitExceeded(technical error, format string, args ...any) error {
	return apierror.Wrapf(technical, CodeBisqLimitExceeded, format, args...).WithStatus(http.StatusConflict)
}
