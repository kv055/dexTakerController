package api

import (
	"commons/apierror"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"dexTakerController/internal/exchangeconnectors/dex/robosats"
)

// Authenticated internal endpoint: only coordinator-native, read-only terms are
// returned. Route selection and quote persistence remain owned by Unified API.
func (s *Server) handleRoboSatsQuote(w http.ResponseWriter, r *http.Request) {
	quoter, ok := s.tradeFlow.(interface {
		QuoteRoboSats(context.Context, robosats.QuoteInput) (*robosats.NativeTerms, error)
	})
	if !ok {
		writeError(w, http.StatusServiceUnavailable, robosats.ErrDisabled())
		return
	}
	var input robosats.QuoteInput
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192)).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid RoboSats quote request: %w", err))
		return
	}
	terms, err := quoter.QuoteRoboSats(r.Context(), input)
	if err != nil {
		// The Unified API relays only the message; keep the full cause here.
		slog.Warn("RoboSats quote rejected", "coordinator", input.CoordinatorID, "offer_id", input.OrderID,
			"amount", input.Amount, "unit", input.Unit, "payment_method", input.PaymentMethod,
			"code", apierror.CodeOf(err), "err", err)
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, terms)
}
