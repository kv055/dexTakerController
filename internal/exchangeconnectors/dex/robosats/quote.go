package robosats

import (
	"commons/apierror"
	"commons/fiat"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type QuoteInput struct {
	CoordinatorID string `json:"venue_coordinator_id"`
	OrderID       string `json:"offer_id"`
	Amount        string `json:"amount"`
	Unit          string `json:"unit"`
	Currency      string `json:"fiat_currency"`
	PaymentMethod string `json:"payment_method"`
}
type NativeTerms struct {
	CoordinatorID string    `json:"venue_coordinator_id"`
	OrderID       string    `json:"offer_id"`
	FiatAmount    string    `json:"fiat_amount"`
	Currency      string    `json:"fiat_currency"`
	PaymentMethod string    `json:"payment_method"`
	Price         string    `json:"price"`
	BondPercent   string    `json:"bond_percent"`
	TakerFee      string    `json:"taker_fee"`
	PrincipalSat  int64     `json:"principal_sat"`
	MaxBondSat    int64     `json:"max_bond_sat"`
	MaxEscrowSat  int64     `json:"max_escrow_sat"`
	NodePubKey    string    `json:"node_pubkey"`
	ExpiresAt     time.Time `json:"expires_at"`
}

var decimalPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

func decimal(s string) (*big.Rat, error) {
	if !decimalPattern.MatchString(s) {
		return nil, fmt.Errorf("invalid decimal")
	}
	v, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("invalid decimal")
	}
	return v, nil
}
func ceilSats(v *big.Rat) (int64, error) {
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(v.Num(), v.Denom(), r)
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() || q.Sign() <= 0 || q.Cmp(big.NewInt(2100000000000000)) > 0 {
		return 0, fmt.Errorf("amount outside Bitcoin supply")
	}
	return q.Int64(), nil
}

// Quote uses authenticated fresh order/info responses, never the CEX reference price.
func (c *Client) Quote(ctx context.Context, in QuoteInput) (*NativeTerms, error) {
	robot, err := c.GetRobot(ctx)
	if err != nil {
		return nil, explainCoordinatorError(err, "load your robot")
	}
	if active := robotString(robot, "active_order_id"); active != "" {
		return nil, robotBusy(active)
	}
	p, err := c.GetPosition(ctx, in.OrderID)
	if err != nil {
		return nil, explainCoordinatorError(err, "load this offer")
	}
	var info struct {
		Network string      `json:"network"`
		Node    string      `json:"node_id"`
		Fee     json.Number `json:"taker_fee"`
	}
	if err = c.request(ctx, http.MethodGet, "/api/info/", nil, &info); err != nil {
		return nil, explainCoordinatorError(err, "load its fee and node information")
	}
	if info.Network != "mainnet" || len(info.Node) != 66 {
		return nil, apierror.Newf(CodeCoordinatorUnverified,
			"The RoboSats coordinator did not confirm a mainnet Lightning node (network %q), so its invoices can't be verified. Try again later or pick an offer from another coordinator.", info.Network).
			WithStatus(http.StatusConflict)
	}
	return quoteFromPosition(p, in, info.Fee.String(), info.Node, time.Now())
}
func quoteFromPosition(p *Position, in QuoteInput, feeString, node string, now time.Time) (*NativeTerms, error) {
	if err := offerStateProblem(p, in.OrderID); err != nil {
		return nil, err
	}
	var raw struct {
		Currency int         `json:"currency"`
		Price    json.Number `json:"price_now"`
		Bond     json.Number `json:"bond_size"`
		Password bool        `json:"has_password"`
	}
	if json.Unmarshal(p.Raw, &raw) != nil {
		return nil, apierror.Newf(CodeInvalidTerms, "RoboSats returned offer #%s in a format this bot can't read, so it can't be priced safely. Try again in a moment.", in.OrderID).WithStatus(http.StatusBadGateway)
	}
	if raw.Password {
		return nil, apierror.Newf(CodeOfferPassword, "Offer #%s is password-protected on RoboSats, so it can't be taken automatically. Pick another offer.", in.OrderID).WithStatus(http.StatusConflict)
	}
	if want := fiat.RoboSatsCurrencyID(in.Currency); raw.Currency == 0 || raw.Currency != want {
		return nil, apierror.Newf(CodeCurrencyChanged, "Offer #%s is no longer priced in %s on RoboSats (it now uses %s). Refresh the offer list.", in.OrderID, strings.ToUpper(in.Currency), roboSatsCurrencyName(raw.Currency)).WithStatus(http.StatusConflict)
	}
	if !strings.EqualFold(strings.TrimSpace(p.PaymentMethod), strings.TrimSpace(in.PaymentMethod)) {
		return nil, apierror.Newf(CodePaymentMethod, "The payment method %q doesn't match offer #%s on RoboSats, which lists %q. Pick the offer's own payment method.", strings.TrimSpace(in.PaymentMethod), in.OrderID, strings.TrimSpace(p.PaymentMethod)).WithStatus(http.StatusConflict)
	}
	price, err := decimal(raw.Price.String())
	if err != nil || price.Sign() <= 0 {
		return nil, apierror.Newf(CodeInvalidTerms, "RoboSats didn't return a current price for offer #%s. Try again in a moment.", in.OrderID).WithStatus(http.StatusBadGateway)
	}
	bond, err := decimal(raw.Bond.String())
	if err != nil || bond.Sign() <= 0 || bond.Cmp(big.NewRat(100, 1)) > 0 {
		return nil, apierror.Newf(CodeInvalidTerms, "RoboSats returned an impossible bond size (%q%%) for offer #%s, so it can't be priced safely. Pick another offer.", raw.Bond.String(), in.OrderID).WithStatus(http.StatusConflict)
	}
	fee, err := decimal(feeString)
	if err != nil || fee.Cmp(big.NewRat(1, 1)) >= 0 {
		return nil, apierror.Newf(CodeInvalidTerms, "The RoboSats coordinator reported an impossible taker fee (%q), so its offers can't be priced safely. Try again later or pick another coordinator.", feeString).WithStatus(http.StatusConflict)
	}
	amount, err := decimal(in.Amount)
	if err != nil || amount.Sign() <= 0 {
		return nil, apierror.Newf(CodeInvalidAmount, "Enter an amount greater than zero (got %q).", in.Amount).WithStatus(http.StatusBadRequest)
	}
	if in.Unit == "btc" {
		amount.Mul(amount, price)
	} else if in.Unit != "fiat" {
		return nil, apierror.Newf(CodeInvalidAmount, "The amount unit must be BTC or fiat (got %q).", in.Unit).WithStatus(http.StatusBadRequest)
	}
	// Fiat precision is explicitly confirmed to two decimals; never truncate silently.
	fiatString := amount.FloatString(2)
	amount, err = decimal(fiatString)
	if err != nil || amount.Sign() <= 0 {
		return nil, apierror.Newf(CodeInvalidAmount, "That amount is less than 0.01 %s at this offer's price. Enter a larger amount.", strings.ToUpper(in.Currency)).WithStatus(http.StatusBadRequest)
	}
	min, max := p.Amount.String(), p.Amount.String()
	if p.HasRange {
		min, max = p.MinAmount.String(), p.MaxAmount.String()
	}
	lo, e1 := decimal(min)
	hi, e2 := decimal(max)
	if e1 != nil || e2 != nil {
		return nil, apierror.Newf(CodeInvalidTerms, "RoboSats didn't return the amount limits of offer #%s. Try again in a moment.", in.OrderID).WithStatus(http.StatusBadGateway)
	}
	if amount.Cmp(lo) < 0 || amount.Cmp(hi) > 0 {
		cur := strings.ToUpper(in.Currency)
		if lo.Cmp(hi) == 0 {
			return nil, apierror.Newf(CodeAmountOutOfRange, "Offer #%s is for exactly %s %s; %s %s can't be taken. Enter %s %s.", in.OrderID, min, cur, fiatString, cur, min, cur).WithStatus(http.StatusBadRequest)
		}
		return nil, apierror.Newf(CodeAmountOutOfRange, "%s %s is outside offer #%s's limits of %s–%s %s. Enter an amount within that range.", fiatString, cur, in.OrderID, min, max, cur).WithStatus(http.StatusBadRequest)
	}
	sats := new(big.Rat).Mul(new(big.Rat).Quo(amount, price), big.NewRat(100000000, 1))
	principal, err := ceilSats(sats)
	if err != nil {
		return nil, err
	}
	bondSat, err := ceilSats(new(big.Rat).Mul(big.NewRat(principal, 1), new(big.Rat).Quo(bond, big.NewRat(100, 1))))
	if err != nil {
		return nil, err
	}
	escrow, err := ceilSats(new(big.Rat).Mul(big.NewRat(principal, 1), new(big.Rat).Add(big.NewRat(1, 1), fee)))
	if err != nil {
		return nil, err
	}
	expires, err := time.Parse(time.RFC3339Nano, p.ExpiresAt)
	if err != nil {
		return nil, apierror.Newf(CodeInvalidTerms, "RoboSats didn't say when offer #%s expires, so it can't be taken safely. Try again in a moment.", in.OrderID).WithStatus(http.StatusBadGateway)
	}
	if !expires.After(now.Add(30 * time.Second)) {
		return nil, apierror.Newf(CodeOfferExpiring, "Offer #%s expires in less than 30 seconds, too soon to take it safely. Pick another offer.", in.OrderID).WithStatus(http.StatusConflict)
	}
	return &NativeTerms{CoordinatorID: in.CoordinatorID, OrderID: in.OrderID, FiatAmount: fiatString, Currency: in.Currency, PaymentMethod: in.PaymentMethod, Price: raw.Price.String(), BondPercent: raw.Bond.String(), TakerFee: feeString, PrincipalSat: principal, MaxBondSat: bondSat, MaxEscrowSat: escrow, NodePubKey: node, ExpiresAt: expires}, nil
}

// roboSatsCurrencyName maps a RoboSats currency ID back to its code for messages.
func roboSatsCurrencyName(id int) string {
	for _, code := range []string{"USD", "EUR", "GBP", "AUD", "CAD", "CHF", "SEK", "NOK"} {
		if fiat.RoboSatsCurrencyID(code) == id {
			return code
		}
	}
	if id == 0 {
		return "no currency"
	}
	return fmt.Sprintf("RoboSats currency #%d", id)
}

func (t *NativeTerms) CheckPosition(p *Position, now time.Time) error {
	fresh, err := quoteFromPosition(p, QuoteInput{CoordinatorID: t.CoordinatorID, OrderID: t.OrderID, Amount: t.FiatAmount, Unit: "fiat", Currency: t.Currency, PaymentMethod: t.PaymentMethod}, t.TakerFee, t.NodePubKey, now)
	if err != nil {
		return err
	}
	if fresh.Price != t.Price || fresh.BondPercent != t.BondPercent || fresh.MaxBondSat > t.MaxBondSat || fresh.MaxEscrowSat > t.MaxEscrowSat {
		return apierror.Newf(CodeTermsChanged,
			"Offer #%s changed on RoboSats since your quote (price %s → %s, bond %s%% → %s%%). Review the new quote and confirm again.",
			t.OrderID, t.Price, fresh.Price, t.BondPercent, fresh.BondPercent).WithStatus(http.StatusConflict)
	}
	return nil
}
