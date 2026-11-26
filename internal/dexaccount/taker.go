package dexaccount

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"dexTakerController/internal/db"
	"dexTakerController/internal/exchangeconnectors/dex/bisq"
	"dexTakerController/internal/exchangeconnectors/dex/bisq2"
	"dexTakerController/internal/exchangeconnectors/dex/hodlhodl"
	"dexTakerController/internal/exchangeconnectors/dex/mostro"
	"dexTakerController/internal/exchangeconnectors/dex/robosats"
)

const (
	depositPollInterval = 3 * time.Second
	depositPollTimeout  = 2 * time.Minute
	defaultSatPerVbyte  = 2
)

// LNDRelay sends on-chain BTC, pays a lightning invoice, and reads the confirmed
// balance of a single hub address (to detect an incoming CEX withdrawal).
type LNDRelay interface {
	SendOnChain(ctx context.Context, toAddress string, amountSat, satPerVbyte int64) (string, error)
	PayInvoice(ctx context.Context, paymentRequest string, feeLimitSat int64) (paymentHash string, err error)
	ConfirmedBalanceForAddress(ctx context.Context, address string) (int64, error)
}

// Taker accepts DEX offers and resolves per-offer deposit destinations.
type Taker struct {
	repo         *db.TransactionRepository
	bisq         *bisq.Client
	bisq2        *bisq2.Client
	hodlhodl     *hodlhodl.Client
	robosats     *robosats.Client
	robosatsExec *robosats.Coordinator
	mostro       MostroTaker
}

type MostroTaker interface {
	Take(context.Context, mostro.TakeRequest) (*mostro.TakeResult, error)
	Ready() error
	Check(context.Context, mostro.TakeRequest) error
}

// NewTaker creates a Taker backed by configured DEX clients.
func NewTaker(
	repo *db.TransactionRepository,
	bisqClient *bisq.Client,
	hodlhodlClient *hodlhodl.Client,
	robosatsClient *robosats.Client,
	mostroClient MostroTaker,
) *Taker {
	return &Taker{
		repo:     repo,
		bisq:     bisqClient,
		hodlhodl: hodlhodlClient,
		robosats: robosatsClient,
		mostro:   mostroClient,
	}
}

// WithBisq2 attaches the Bisq Easy REST client used to take bids and resolve
// the buyer's bitcoinPaymentData.
func (t *Taker) WithBisq2(client *bisq2.Client) *Taker {
	t.bisq2 = client
	return t
}

func (t *Taker) WithRoboSats(c *robosats.Coordinator) *Taker {
	if t != nil {
		t.robosatsExec = c
	}
	return t
}

// OfferInput identifies an offer to take.
type OfferInput struct {
	Exchange           string
	OfferID            string
	FiatAmount         float64
	BTCAmount          float64
	RequestID          uint64
	VenueNodePubKey    string
	ProtocolVersion    int
	SourceEventID      string
	FiatCurrency       string
	PaymentMethod      string
	MaxPaymentSat      int64
	MaxBondSat         int64
	RoutingFeeSat      int64
	CoordinatorID      string
	CredentialID       int64
	QuoteID            string
	BondFeeSat         int64
	EscrowFeeSat       int64
	NativeTerms        *robosats.NativeTerms
	PaymentAccountID   string
	MaxWalletDebitSats uint64
	MinVolumeAsLong    int64
}

// TakeResult is the outcome of TakeOffer plus optional deposit destination.
type TakeResult struct {
	Execution      map[string]any
	DepositAddress string
	LNInvoice      string
}

// Take accepts the offer on the given DEX and returns execution metadata.
func (t *Taker) Take(ctx context.Context, in OfferInput) (*TakeResult, error) {
	offerID := strings.TrimSpace(in.OfferID)
	if offerID == "" {
		return nil, fmt.Errorf("dexaccount: offer_id is required")
	}

	switch db.DexPlatformFromSlug(in.Exchange) {
	case db.DexHodlHodl:
		return t.takeHodlHodl(ctx, offerID, in.FiatAmount)
	case db.DexBisq:
		return t.takeBisq(ctx, in)
	case db.DexBisq2:
		return t.takeBisq2(ctx, in)
	case db.DexRoboSats:
		return t.takeRoboSats(ctx, in)
	case db.DexMostro:
		return t.takeMostro(ctx, in)
	default:
		return nil, fmt.Errorf("dexaccount: unsupported exchange %q", in.Exchange)
	}
}

func (t *Taker) MostroReady() error {
	if t.mostro == nil {
		return fmt.Errorf("mostro client is not configured")
	}
	return t.mostro.Ready()
}
func (t *Taker) CheckMostro(ctx context.Context, in OfferInput) error {
	if err := t.MostroReady(); err != nil {
		return err
	}
	if math.IsNaN(in.FiatAmount) || math.IsInf(in.FiatAmount, 0) || in.FiatAmount <= 0 || in.FiatAmount >= float64(math.MaxInt64) || math.Trunc(in.FiatAmount) != in.FiatAmount {
		return fmt.Errorf("mostro: fiat amount must be a positive integer")
	}
	return t.mostro.Check(ctx, mostro.TakeRequest{RequestID: 1, Node: in.VenueNodePubKey, OrderID: in.OfferID, SourceEventID: in.SourceEventID, Protocol: in.ProtocolVersion, FiatAmount: int64(in.FiatAmount), FiatCurrency: in.FiatCurrency, PaymentMethod: in.PaymentMethod, MaxEscrowSat: in.MaxPaymentSat, MaxBondSat: in.MaxBondSat, RoutingFeeSat: in.RoutingFeeSat})
}
func (t *Taker) takeMostro(ctx context.Context, in OfferInput) (*TakeResult, error) {
	if err := t.MostroReady(); err != nil {
		return nil, err
	}
	if math.IsNaN(in.FiatAmount) || math.IsInf(in.FiatAmount, 0) || in.FiatAmount <= 0 || in.FiatAmount >= float64(math.MaxInt64) || math.Trunc(in.FiatAmount) != in.FiatAmount {
		return nil, fmt.Errorf("mostro: selected fiat amount must be a positive integer")
	}
	result, err := t.mostro.Take(ctx, mostro.TakeRequest{RequestID: in.RequestID, Node: in.VenueNodePubKey, OrderID: in.OfferID, SourceEventID: in.SourceEventID, Protocol: in.ProtocolVersion, FiatAmount: int64(in.FiatAmount), FiatCurrency: in.FiatCurrency, PaymentMethod: in.PaymentMethod, MaxEscrowSat: in.MaxPaymentSat, MaxBondSat: in.MaxBondSat, RoutingFeeSat: in.RoutingFeeSat})
	if err != nil {
		return nil, err
	}
	if result == nil || !result.Funded {
		return nil, fmt.Errorf("mostro: escrow acceptance has not been confirmed")
	}
	return &TakeResult{Execution: map[string]any{"exchange": "mostro", "offer_id": in.OfferID, "request_id": in.RequestID, "mostro_session_id": result.SessionID, "mostro_trade_ref": fmt.Sprintf("%s:%s:%d", in.VenueNodePubKey, in.OfferID, in.RequestID), "mostro_funded": true, "payment_hash": result.PaymentHash, "amount_sat": result.AmountSat, "venue_node_pubkey": in.VenueNodePubKey}}, nil
}

// ResolveDeposit polls non-direct venue APIs until an on-chain address or LN
// invoice is available. Bisq is pre-funded through its integrated wallet and
// therefore has no per-trade destination to resolve.
func (t *Taker) ResolveDeposit(ctx context.Context, exchange string, execution map[string]any) (address, invoice string, err error) {
	if db.DexPlatformFromSlug(exchange) == db.DexMostro {
		return "", "", fmt.Errorf("mostro: funding must be reconciled by the Mostro coordinator")
	}
	if execution != nil && execution["robosats_funded"] == true {
		return "", "", nil
	}
	if execution == nil {
		return "", "", fmt.Errorf("dexaccount: execution payload is required")
	}
	if addr := StringField(execution, "escrow_address"); addr != "" {
		return addr, "", nil
	}
	if addr := StringField(execution, "deposit_address"); addr != "" {
		return addr, "", nil
	}
	if inv := StringField(execution, "invoice"); inv != "" {
		return "", inv, nil
	}
	platform := db.DexPlatformFromSlug(exchange)
	if platform == db.DexBisq {
		return "", "", nil
	}
	if platform == db.DexBisq2 {
		return t.pollBisq2Deposit(ctx, execution)
	}
	if platform != db.DexRoboSats {
		return "", "", nil
	}

	deadline := time.Now().Add(depositPollTimeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return "", "", ctx.Err()
		}

		address, invoice, err = t.pollRoboSatsDeposit(ctx, execution)
		if err != nil {
			return "", "", err
		}
		if address != "" || invoice != "" {
			return address, invoice, nil
		}

		select {
		case <-ctx.Done():
			return "", "", ctx.Err()
		case <-time.After(depositPollInterval):
		}
	}
	return "", "", fmt.Errorf("dexaccount: deposit destination not available for %s after %s", exchange, depositPollTimeout)
}

func (t *Taker) takeHodlHodl(ctx context.Context, offerID string, fiatAmount float64) (*TakeResult, error) {
	if t.hodlhodl == nil {
		return nil, fmt.Errorf("hodlhodl client is not configured")
	}
	fiatStr := strconv.FormatFloat(fiatAmount, 'f', 2, 64)
	escrowAddress, contract, err := t.hodlhodl.TakeOffer(ctx, offerID, fiatStr)
	if err != nil {
		return nil, err
	}
	return &TakeResult{
		Execution: map[string]any{
			"exchange":       "hodlhodl",
			"offer_id":       offerID,
			"fiat_amount":    fiatStr,
			"escrow_address": escrowAddress,
			"contract_id":    contract.ID,
			"contract":       contract,
		},
		DepositAddress: escrowAddress,
	}, nil
}

func (t *Taker) takeBisq(ctx context.Context, in OfferInput) (*TakeResult, error) {
	if strings.TrimSpace(in.PaymentAccountID) == "" {
		return nil, fmt.Errorf("bisq: payment_account_id is required")
	}
	if t.bisq == nil {
		return nil, fmt.Errorf("bisq client is not configured")
	}
	amountSats := uint64(BTCToSats(in.BTCAmount))
	if amountSats == 0 {
		return nil, fmt.Errorf("bisq: btc amount is required")
	}
	trade, err := t.bisq.GuardedTakeOffer(ctx, in.OfferID, bisq.GuardedTakeOfferParams{
		PaymentAccountID:   in.PaymentAccountID,
		AmountSats:         amountSats,
		MaxWalletDebitSats: in.MaxWalletDebitSats,
		MinVolumeAsLong:    in.MinVolumeAsLong,
	})
	if err != nil {
		return nil, err
	}
	fiatAmount := bisq.FiatAmountFromVolumeAsLong(parseVolumeAsLong(trade.TradeVolume))
	if trade.TradeAmountSats > 0 && trade.TradeAmountSats != amountSats {
		return nil, fmt.Errorf("bisq: taken amount %d sats does not match confirmed %d sats", trade.TradeAmountSats, amountSats)
	}
	return &TakeResult{
		Execution: map[string]any{
			"exchange":           "bisq",
			"offer_id":           in.OfferID,
			"trade_id":           trade.TradeID,
			"trade":              trade,
			"requested_sats":     amountSats,
			"trade_amount_sats":  trade.TradeAmountSats,
			"trade_price":        trade.TradePrice,
			"trade_volume":       trade.TradeVolume,
			"fiat_amount":        fiatAmount,
			"payment_account_id": in.PaymentAccountID,
		},
	}, nil
}

func parseVolumeAsLong(volume string) int64 {
	volume = strings.TrimSpace(volume)
	if volume == "" {
		return 0
	}
	fields := strings.Fields(volume)
	if len(fields) == 0 {
		return 0
	}
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	return int64(math.Round(value * 10000))
}

func (t *Taker) takeBisq2(ctx context.Context, in OfferInput) (*TakeResult, error) {
	if t.bisq2 == nil {
		return nil, fmt.Errorf("bisq2 client is not configured")
	}
	quoteUnits := bisq2.QuoteAmountUnits(in.FiatAmount)
	baseSats := BTCToSats(in.BTCAmount)
	if quoteUnits <= 0 {
		return nil, fmt.Errorf("bisq2: fiat amount is required to take an offer")
	}
	if baseSats <= 0 {
		return nil, fmt.Errorf("bisq2: btc amount is required to take an offer")
	}

	offer, err := t.bisq2.GetOffer(ctx, in.FiatCurrency, in.OfferID)
	if err != nil {
		return nil, err
	}
	bitcoinMethod := bisq2.PickBitcoinRail(offer.BitcoinRails)
	fiatMethod := bisq2.MatchFiatRail(in.PaymentMethod, offer.FiatRails)
	if fiatMethod == "" {
		return nil, fmt.Errorf("bisq2: no fiat payment method selected for offer %s", in.OfferID)
	}

	taken, err := t.bisq2.TakeOffer(ctx, in.OfferID, bisq2.TakeOfferParams{
		BaseSideAmount:       baseSats,
		QuoteSideAmount:      quoteUnits,
		BitcoinPaymentMethod: bitcoinMethod,
		FiatPaymentMethod:    fiatMethod,
	})
	if err != nil {
		return nil, err
	}

	if accountData := t.bisq2PaymentAccountData(ctx, in.PaymentMethod); accountData != "" {
		if err := t.bisq2.SendSellerPaymentAccount(ctx, taken.TradeID, accountData); err != nil {
			log.Printf("dexaccount: bisq2 send seller payment account for trade %s: %v", taken.TradeID, err)
		}
	}

	return &TakeResult{
		Execution: map[string]any{
			"exchange":               "bisq2",
			"offer_id":               in.OfferID,
			"trade_id":               taken.TradeID,
			"requested_sats":         baseSats,
			"quote_side_amount":      quoteUnits,
			"bitcoin_payment_method": taken.BitcoinPaymentMethod,
			"fiat_payment_method":    taken.FiatPaymentMethod,
		},
	}, nil
}

func (t *Taker) bisq2PaymentAccountData(ctx context.Context, paymentMethod string) string {
	if t.repo == nil {
		return ""
	}
	vars, err := t.repo.GetChatTemplateVariables(ctx, paymentMethod)
	if err != nil {
		log.Printf("dexaccount: bisq2 payment account variables: %v", err)
		return ""
	}
	return formatBisq2PaymentAccount(paymentMethod, vars)
}

func formatBisq2PaymentAccount(method string, vars map[string]string) string {
	if vars == nil {
		vars = map[string]string{}
	}
	var b strings.Builder
	if method = strings.TrimSpace(method); method != "" {
		b.WriteString(method)
	}
	writeLine := func(label, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(label)
		b.WriteString(": ")
		b.WriteString(value)
	}
	writeLine("Account holder", vars["owner_name"])
	if details := strings.TrimSpace(vars["details_encrypted"]); details != "" && details != "{}" && details != "null" {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(details)
	}
	writeLine("Email", vars["email"])
	writeLine("Phone", vars["phone"])
	writeLine("Payment tag", vars["payment_tag"])
	return strings.TrimSpace(b.String())
}

func (t *Taker) pollBisq2Deposit(ctx context.Context, execution map[string]any) (string, string, error) {
	if t.bisq2 == nil {
		return "", "", fmt.Errorf("bisq2 client is not configured")
	}
	tradeID := StringField(execution, "trade_id")
	if tradeID == "" {
		return "", "", fmt.Errorf("bisq2: trade_id missing from execution")
	}
	data, err := t.bisq2.WaitForBitcoinPaymentData(ctx, tradeID, bisq2.DepositPollTimeout)
	if err != nil {
		return "", "", err
	}
	addr, invoice := bisq2.ClassifyBitcoinPaymentData(data)
	if addr == "" && invoice == "" {
		return "", "", fmt.Errorf("bisq2: bitcoin payment data for trade %s is empty", tradeID)
	}
	return addr, invoice, nil
}

// ReportBtcSent notifies a venue that we have sent the BTC leg. Bisq Easy needs
// SELLER_CONFIRM_BTC_SENT with the on-chain txid (or LN proof).
func (t *Taker) ReportBtcSent(ctx context.Context, exchange, tradeID, proof string) error {
	if db.DexPlatformFromSlug(exchange) != db.DexBisq2 {
		return nil
	}
	if t.bisq2 == nil {
		return fmt.Errorf("bisq2 client is not configured")
	}
	if err := t.bisq2.ConfirmBtcSent(ctx, tradeID, proof); err != nil {
		return fmt.Errorf("bisq2: confirm btc sent: %w", err)
	}
	return nil
}

func (t *Taker) CheckRoboSats(ctx context.Context, in OfferInput) error {
	if t.robosatsExec == nil {
		return fmt.Errorf("robosats: execution coordinator is not configured")
	}
	if t.repo != nil && in.RequestID > 0 {
		if session, err := t.repo.LoadRoboSatsSession(ctx, in.RequestID); err != nil {
			return err
		} else if session != nil {
			return t.robosatsExec.CheckBound(ctx, session)
		}
	}
	fiat := strconv.FormatFloat(in.FiatAmount, 'f', -1, 64)
	if math.Trunc(in.FiatAmount) == in.FiatAmount {
		fiat = strconv.FormatFloat(in.FiatAmount, 'f', 0, 64)
	}
	if in.CredentialID == 0 {
		id, _, err := t.latestRoboSatsCredential(ctx)
		if err != nil {
			return err
		}
		in.CredentialID = id
	}
	return t.robosatsExec.Check(ctx, robosats.TakeRequest{
		RequestID: in.RequestID, CoordinatorID: in.CoordinatorID, OrderID: in.OfferID,
		FiatAmount: fiat, FiatCurrency: in.FiatCurrency, PaymentMethod: in.PaymentMethod,
		MaxBondSat: in.MaxBondSat, MaxEscrowSat: in.MaxPaymentSat, RoutingFeeSat: in.RoutingFeeSat,
		CredentialID: in.CredentialID, QuoteID: in.QuoteID,
		Terms: in.NativeTerms, BondFeeSat: in.BondFeeSat, EscrowFeeSat: in.EscrowFeeSat,
	})
}

func (t *Taker) QuoteRoboSats(ctx context.Context, in robosats.QuoteInput) (*robosats.NativeTerms, error) {
	if t.robosatsExec == nil || !t.robosatsExec.Enabled {
		return nil, robosats.ErrDisabled()
	}
	_, token, err := t.latestRoboSatsCredential(ctx)
	if err != nil {
		return nil, fmt.Errorf("load RoboSats robot credential: %w", err)
	}
	if strings.TrimSpace(token) == "" {
		return nil, robosats.ErrNoRobot()
	}
	api, err := t.robosatsExec.Dial(in.CoordinatorID, token)
	if err != nil {
		return nil, robosats.ErrCoordinatorUnknown(in.CoordinatorID, err)
	}
	client, ok := api.(*robosats.Client)
	if !ok {
		return nil, fmt.Errorf("RoboSats quote client unavailable (dialer returned %T)", api)
	}
	return client.Quote(ctx, in)
}

func (t *Taker) PrepareRoboSats(ctx context.Context, in OfferInput) (*robosats.Session, error) {
	if err := t.CheckRoboSats(ctx, in); err != nil {
		return nil, err
	}
	id, _, err := t.latestRoboSatsCredential(ctx)
	if err != nil {
		return nil, err
	}
	s := &robosats.Session{Phase: robosats.PhaseCreated, CreatedAt: time.Now().UTC(), Payments: map[string]*robosats.Payment{}, Request: robosats.TakeRequest{
		RequestID: in.RequestID, CoordinatorID: in.CoordinatorID, OrderID: in.OfferID, FiatAmount: strconv.FormatFloat(in.FiatAmount, 'f', 2, 64), FiatCurrency: in.FiatCurrency, PaymentMethod: in.PaymentMethod, CredentialID: id, QuoteID: in.QuoteID, MaxBondSat: in.MaxBondSat, MaxEscrowSat: in.MaxPaymentSat, RoutingFeeSat: in.RoutingFeeSat, BondFeeSat: in.BondFeeSat, EscrowFeeSat: in.EscrowFeeSat, Terms: in.NativeTerms,
	}}
	if in.NativeTerms == nil {
		return nil, fmt.Errorf("RoboSats native quote terms missing")
	}
	s.Request.NodePubKey = in.NativeTerms.NodePubKey
	if err := t.robosatsExec.SnapshotIdentity(ctx, s); err != nil {
		return nil, err
	}
	return s, nil
}

func (t *Taker) RoboSatsLiquidityPrepared(ctx context.Context, id uint64, mark bool) (bool, error) {
	if t.robosatsExec == nil {
		return false, fmt.Errorf("RoboSats coordinator unavailable")
	}
	store := t.robosatsExec.Store
	unlock, err := store.LockRoboSats(ctx, id)
	if err != nil {
		return false, err
	}
	defer unlock()
	s, err := store.LoadRoboSatsSession(ctx, id)
	if err != nil {
		return false, err
	}
	if s == nil {
		return false, fmt.Errorf("RoboSats admitted session missing")
	}
	if mark {
		s.PreparationComplete = true
		err = store.SaveRoboSatsSession(ctx, s)
	}
	return s.PreparationComplete, err
}

func (t *Taker) takeRoboSats(ctx context.Context, in OfferInput) (*TakeResult, error) {
	if t.robosatsExec != nil {
		if t.repo != nil {
			s, err := t.repo.LoadRoboSatsSession(ctx, in.RequestID)
			if err != nil {
				return nil, err
			}
			if s != nil {
				if s.Request.OrderID != in.OfferID || s.Request.CoordinatorID != in.CoordinatorID {
					return nil, fmt.Errorf("RoboSats bound offer mismatch")
				}
				result, err := t.robosatsExec.Take(ctx, s.Request)
				if err != nil {
					return nil, err
				}
				return roboTakeResult(result, in)
			}
		}
		if in.CredentialID == 0 {
			id, _, err := t.latestRoboSatsCredential(ctx)
			if err != nil {
				return nil, err
			}
			in.CredentialID = id
		}
		fiat := strconv.FormatFloat(in.FiatAmount, 'f', -1, 64)
		result, err := t.robosatsExec.Take(ctx, robosats.TakeRequest{
			RequestID: in.RequestID, CoordinatorID: in.CoordinatorID, OrderID: in.OfferID,
			FiatAmount: fiat, FiatCurrency: in.FiatCurrency, PaymentMethod: in.PaymentMethod,
			MaxBondSat: in.MaxBondSat, MaxEscrowSat: in.MaxPaymentSat, RoutingFeeSat: in.RoutingFeeSat,
			CredentialID: in.CredentialID, QuoteID: in.QuoteID,
		})
		if err != nil {
			return nil, err
		}
		return roboTakeResult(result, in)
	}
	if t.robosats == nil {
		return nil, fmt.Errorf("robosats client is not configured")
	}
	client := t.robosats
	if token, err := t.latestRoboSatsToken(ctx); err != nil {
		log.Printf("dexaccount: failed to refresh Robosats token from database; retaining current database-loaded client token: %v", err)
	} else if token != "" {
		client = t.robosats.WithAuthToken(token)
	}
	fiat := strconv.FormatFloat(in.FiatAmount, 'f', -1, 64)
	position, err := client.TakeOfferWithAmount(ctx, in.OfferID, fiat)
	if err != nil {
		return nil, err
	}
	exec := map[string]any{
		"exchange":    "robosats",
		"offer_id":    in.OfferID,
		"position_id": position.ID,
		"position":    position,
	}
	addr, inv := extractRoboSatsDeposit(position)
	return &TakeResult{
		Execution:      exec,
		DepositAddress: addr,
		LNInvoice:      inv,
	}, nil
}

func roboTakeResult(result *robosats.TakeResult, in OfferInput) (*TakeResult, error) {
	if result == nil || !result.Funded {
		return nil, fmt.Errorf("robosats: seller escrow has not been confirmed")
	}
	return &TakeResult{Execution: map[string]any{"exchange": "robosats", "offer_id": in.OfferID, "position_id": result.OrderID, "coordinator_id": in.CoordinatorID, "robosats_session_id": result.SessionID, "robosats_trade_ref": robosats.TradeRef(in.CoordinatorID, result.OrderID), "robosats_funded": true, "payment_hash": result.PaymentHash, "amount_sat": result.AmountSat, "phase": result.Phase}}, nil
}

func (t *Taker) pollRoboSatsDeposit(ctx context.Context, execution map[string]any) (string, string, error) {
	positionID := StringField(execution, "position_id")
	if positionID == "" {
		return "", "", fmt.Errorf("robosats: position_id missing from execution")
	}
	client := t.robosats
	if token, err := t.latestRoboSatsToken(ctx); err == nil && token != "" {
		client = t.robosats.WithAuthToken(token)
	}
	position, err := client.GetPosition(ctx, positionID)
	if err != nil {
		return "", "", err
	}
	addr, inv := extractRoboSatsDeposit(position)
	return addr, inv, nil
}

func (t *Taker) latestRoboSatsCredential(ctx context.Context) (int64, string, error) {
	if t.repo == nil {
		return 0, "", nil
	}
	keys, err := t.repo.GetActiveAPIKeysForProvider(ctx, "Robosats")
	if err != nil {
		return 0, "", err
	}
	for _, key := range keys {
		if token := strings.TrimSpace(key.APIKey); token != "" {
			return int64(key.ID), token, nil
		}
	}
	return 0, "", nil
}

func (t *Taker) latestRoboSatsToken(ctx context.Context) (string, error) {
	_, token, err := t.latestRoboSatsCredential(ctx)
	return token, err
}

func extractRoboSatsDeposit(position *robosats.Position) (address, invoice string) {
	if position == nil {
		return "", ""
	}
	for _, inv := range []string{position.EscrowInvoice, position.BondInvoice} {
		if looksLikeBolt11(inv) {
			return "", inv
		}
	}
	if len(position.Raw) > 0 {
		var raw map[string]any
		if err := json.Unmarshal(position.Raw, &raw); err == nil {
			for _, key := range []string{"escrow_invoice", "bond_invoice", "invoice", "bolt11", "ln_invoice", "payout_invoice"} {
				if inv := StringField(raw, key); looksLikeBolt11(inv) {
					return "", inv
				}
			}
			for _, key := range []string{"escrow_address", "deposit_address", "address", "bond_address"} {
				if addr := StringField(raw, key); LooksLikeOnChainAddress(addr) {
					return addr, ""
				}
			}
		}
	}
	return "", ""
}

func looksLikeBolt11(inv string) bool {
	lower := strings.ToLower(strings.TrimSpace(inv))
	return strings.HasPrefix(lower, "lnbc") || strings.HasPrefix(lower, "lntb") || strings.HasPrefix(lower, "lnbcrt")
}

// DefaultSatPerVbyte is the conservative on-chain fee rate used for LND→DEX relay.
func DefaultSatPerVbyte() int64 {
	return defaultSatPerVbyte
}
