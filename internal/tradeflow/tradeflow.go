package tradeflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"commons/apierror"
	"commons/mmcapabilities"
	"dexTakerController/internal/cexaccount"
	"dexTakerController/internal/db"
	"dexTakerController/internal/dexaccount"
	"dexTakerController/internal/exchangeconnectors/dex/robosats"
	"dexTakerController/internal/metrics"
	"dexTakerController/internal/pretrade"
	lnd "dexTakerController/internal/wallethub"
)

// Tunables for the guarded hedge retry. A hedge failure after the spot buy leaves
// the account holding unhedged BTC, so we retry a few times — but only after
// confirming with the venue that the failed attempt did not actually open the
// position (avoiding a double-open from a lost confirmation).
const (
	hedgeMaxAttempts = 3
	hedgeRetryDelay  = 750 * time.Millisecond
	// hedgeOpenedFraction is how much of the requested notional must appear on the
	// venue (vs. the pre-attempt baseline) to conclude the order actually filled.
	hedgeOpenedFraction = 0.9
)

// Order carries the parameters for a single arbitrage trade cycle. All fields
// must be supplied by the caller — hedge sizing and venue selection live in
// dexUnifiedApi/commons, not here.
type Order struct {
	// Provider is the CEX to trade on, e.g. "binance".
	Provider string
	// MarketSide is "bid" (buy BTC on CEX) or "ask" (sell BTC on CEX).
	MarketSide string
	// BTCAmount is the spot buy size in BTC.
	BTCAmount float64
	// FiatCurrency is the quote currency for the spot buy, e.g. "EUR".
	FiatCurrency string
	// RequiredCapital is the fiat capital needed (units of FiatCurrency): for
	// hedgefull, spot notional + hedge collateral + security deposit; for
	// hedgeless exchange-fiat check, fiat collateral only.
	RequiredCapital float64
	// RequiredBTCLiquidity is position plus security deposit. Used for hedgeless
	// LND pool reservation and for Swap_main2lightning Lightning liquidity checks
	// in any trade mode.
	RequiredBTCLiquidity float64
	// SecurityDepositBTC is carried separately for audit/UI detail.
	SecurityDepositBTC float64
	// HedgeVenue is the futures venue, e.g. "binance_futures".
	HedgeVenue string
	// HedgeSide is "short" for a bid (we bought spot, now short the perp).
	HedgeSide string
	// HedgeNotionalBTC is the BTC notional to hedge.
	HedgeNotionalBTC float64
	// Leverage is the leverage to use on the hedge (Binance only; ignored by others).
	Leverage float64

	// OfferID is the DEX-native offer to execute. Hedgefull reaches it after the
	// CEX-funded wallet leg; hedgeless spends the pre-funded LND pool first.
	OfferID string
	// Dex is the DEX venue slug, e.g. "hodlhodl".
	Dex string
	// FiatAmount is the fiat notional for TakeOffer (HodlHodl).
	FiatAmount float64
	// TransactionID optionally updates an existing transaction row instead of creating one.
	TransactionID uint
	// SelectedOffer is an optional JSON snapshot of the chosen offer from the UI.
	SelectedOffer []byte
	// QuoteID is the Unified API execution quote bound to this trade.
	QuoteID string
	// PaymentMethod is the canonical offer payment method the operator will use to pay the maker.
	PaymentMethod string
	// PaymentMethods is the offer's full accepted-method list; when PaymentMethod
	// is empty the highest-priority linked+active catalog entry is picked from it.
	PaymentMethods []string
	// TradeMode selects pre-trade permission semantics (hedgefull vs hedgeless).
	// Empty defaults to hedgefull_single_collateral.
	TradeMode string
	// ResolvedRoute is the immutable Unified API routing decision for this offer.
	// New HTTP executions require it; the small legacy fallback exists only for
	// in-process callers and old persisted transactions during rollout.
	ResolvedRoute *ResolvedRoute
	// RoutingFeeAllowanceSat is reserved and passed to LND for Lightning DEX
	// payments. Zero is normalized to the conservative default.
	RoutingFeeAllowanceSat int64
	// LightningLiquiditySource is the transactional setting snapshotted by the
	// API immediately before execution.
	LightningLiquiditySource string
	// CEXWithdrawalFeeBTC and SwapSafetyHeadroomBTC are explicit gross-up
	// inputs used by hedgefull RoboSats preparation.
	CEXWithdrawalFeeBTC   float64
	SwapSafetyHeadroomBTC float64
	// BisqPlan is the accepted Bisq v1 quote used to fund and take without
	// deriving amounts from CEX fills or wallet totals.
	BisqPlan *db.BisqExecutionPlan
}

type ResolvedRoute = db.ResolvedRoute

// Result bundles the outcomes of all steps in the CEX→wallet trade cycle.
type Result struct {
	TradeMode     string                      `json:"trade_mode"`
	SpotBuy       *cexaccount.SpotOrderResult `json:"spot_buy,omitempty"`
	Hedge         *cexaccount.HedgeOpenResult `json:"hedge,omitempty"`
	Withdrawal    *WithdrawalResult           `json:"withdrawal,omitempty"`
	DEXFunding    *DEXFundingResult           `json:"dex_funding,omitempty"`
	PoolRefill    *PoolRefillResult           `json:"pool_refill,omitempty"`
	TransactionID uint                        `json:"transaction_id,omitempty"`
}

// DEXFundingResult describes the BTC commitment made from the LND pool.
type DEXFundingResult struct {
	Exchange    string  `json:"exchange"`
	Method      string  `json:"method"`
	AmountBTC   float64 `json:"amount_btc"`
	TxID        string  `json:"txid,omitempty"`
	PaymentHash string  `json:"payment_hash,omitempty"`
	Destination string  `json:"destination,omitempty"`
	Status      string  `json:"status"`
}

// PoolRefillResult reports the replacement spot/withdrawal state. Restoration
// is confirmed asynchronously from the LND wallet balance.
type PoolRefillResult struct {
	TargetBTC  float64 `json:"target_btc"`
	Status     string  `json:"status"`
	Attempt    int     `json:"attempt"`
	CurrentBTC float64 `json:"current_btc,omitempty"`
}

// WithdrawalResult is the trade-relevant subset of the withdrawal response.
type WithdrawalResult struct {
	Provider        string  `json:"provider"`
	WithdrawalID    string  `json:"withdrawal_id"`
	ClientRequestID string  `json:"client_request_id,omitempty"`
	WalletAddress   string  `json:"wallet_address"`
	AmountBTC       float64 `json:"amount_btc"`
	CexStatus       string  `json:"cex_status,omitempty"`
}

// Service is the subset of cexaccount.Service used by TradeFlow.
type Service interface {
	PreTradeCheck(ctx context.Context, p pretrade.Params) (*pretrade.Report, error)
	MarketBuyBTC(ctx context.Context, btcAmount float64, p pretrade.Params) (*cexaccount.SpotOrderResult, error)
	OpenHedge(ctx context.Context, venue, side string, btcNotional, leverage float64) (*cexaccount.HedgeOpenResult, error)
	LiveHedgePosition(ctx context.Context, venue string) (*cexaccount.HedgeSnapshot, error)
	WithdrawSpotToLND(ctx context.Context, rawProvider string, amountBTC float64, lndAddress, clientRequestID string) (*lnd.WithdrawFromExchangeResult, error)
	WithdrawalAlreadySent(ctx context.Context, rawProvider, clientRequestID string, amountBTC float64, address string, within time.Duration) (*cexaccount.WithdrawalStatus, error)
}

// DirectDestinationResolver resolves the on-chain address the CEX should withdraw to
// for a direct-routed DEX (bisq v1: its integrated wallet funding address), where the
// CEX funds the DEX wallet straight away with no LND hub hop. Only consulted for the
// direct route.
type DirectDestinationResolver interface {
	DirectDestination(ctx context.Context, dexSlug string) (string, error)
}

type directBalanceResolver interface {
	DirectDestinationResolver
	// DirectBalanceBTC lets the Bisq path prove that this trade's wallet funding
	// arrived before it asks Bisq to take the offer.
	DirectBalanceBTC(ctx context.Context, dexSlug, address string) (float64, error)
}

// hedgelessDEX is the venue operation subset used by the LND-funded path.
type hedgelessDEX interface {
	Take(ctx context.Context, in dexaccount.OfferInput) (*dexaccount.TakeResult, error)
	ResolveDeposit(ctx context.Context, exchange string, execution map[string]any) (string, string, error)
}

// hedgelessWallet is deliberately separate from dexaccount.LNDRelay so the
// existing hedgefull relay remains independently testable.
type hedgelessWallet interface {
	SendOnChain(ctx context.Context, toAddress string, amountSat, satPerVbyte int64) (string, error)
	PayInvoice(ctx context.Context, paymentRequest string, feeLimitSat int64) (string, error)
}

type invoiceDecoder interface {
	DecodeInvoice(context.Context, string) (*lnd.DecodedInvoice, error)
}

type chainConfirmationSource interface {
	TransactionConfirmations(ctx context.Context, txid string) (int, error)
}

type liquidityExecutor interface {
	EnsureOutbound(context.Context, string, uint, *pretrade.Report) error
	Release(context.Context, string) error
}

type capabilityStore interface {
	State(context.Context, string) (mmcapabilities.ProviderState, error)
}

// TradeFlow orchestrates the CEX trade leg of a DEX arbitrage cycle.
type TradeFlow struct {
	svc               Service
	store             transactionStore
	directResolver    DirectDestinationResolver
	hedgelessDEX      hedgelessDEX
	hedgelessWallet   hedgelessWallet
	chainSource       chainConfirmationSource
	capabilities      capabilityStore
	liquidity         liquidityExecutor
	robo              *robosats.Coordinator
	roboWorkerEnabled bool
	roboRelay         func(context.Context, *db.Transaction) error
	roboChat          func(context.Context, *db.Transaction) error
	bisqDriver        BisqStepDriver
}

// WithLiquidityExecutor wires trade-triggered submarine swaps. It is invoked
// only when the read-only pre-trade report identifies an outbound shortfall.
func (tf *TradeFlow) WithLiquidityExecutor(executor liquidityExecutor) *TradeFlow {
	tf.liquidity = executor
	return tf
}

// WithCapabilities installs the central fail-closed provider policy gate.
func (tf *TradeFlow) WithCapabilities(store capabilityStore) *TradeFlow {
	tf.capabilities = store
	return tf
}

// WithHedgelessExecution wires the DEX taker and LND wallet used by the
// double-collateral path.
func (tf *TradeFlow) WithHedgelessExecution(dex hedgelessDEX, wallet hedgelessWallet) *TradeFlow {
	tf.hedgelessDEX = dex
	tf.hedgelessWallet = wallet
	return tf
}

// WithChainConfirmationSource wires confirmation lookup for replacement
// withdrawals before a corrective refill is considered.
func (tf *TradeFlow) WithChainConfirmationSource(source chainConfirmationSource) *TradeFlow {
	tf.chainSource = source
	return tf
}

// New creates a TradeFlow backed by the given CEX service and transaction store.
// store may be nil for callers that don't persist (e.g. some tests); persistence
// is also skipped when an order carries no offer_id/dex linkage.
func New(svc Service, store transactionStore) *TradeFlow {
	return &TradeFlow{svc: svc, store: store}
}

// WithDirectDestinationResolver wires the resolver used to find the withdrawal target
// for direct-routed DEXes (bisq v1). Without it, direct-route orders fail fast.
func (tf *TradeFlow) WithDirectDestinationResolver(r DirectDestinationResolver) *TradeFlow {
	tf.directResolver = r
	return tf
}

// Run validates and dispatches a trade. Bid mode selects one of two independent
// money flows: hedgefull buys/hedges on the CEX before funding the DEX, while
// hedgeless funds from existing LND collateral and replenishes it afterward.
func (tf *TradeFlow) Run(ctx context.Context, order Order) (*Result, error) {
	if order.BisqPlan != nil && order.BisqPlan.Version >= 2 {
		return nil, fmt.Errorf("versioned Bisq executions must use the durable step executor")
	}
	if err := validate(order); err != nil {
		return nil, err
	}

	mode := cexaccount.NormalizeTradeMode(order.TradeMode)
	order.TradeMode = mode
	if err := tf.requireCapabilities(ctx, order); err != nil {
		return nil, fmt.Errorf("tradeflow: %w", err)
	}
	switch side := strings.ToLower(strings.TrimSpace(order.MarketSide)); side {
	case "bid", "ask":
		var res *Result
		var err error
		if side == "bid" {
			if mode == cexaccount.TradeModeHedgelessDoubleCollateral {
				res, err = tf.runHedgelessBid(ctx, order)
			} else {
				res, err = tf.runBid(ctx, order)
			}
		} else {
			res, err = tf.runAsk(ctx, order)
		}
		metrics.TradeflowRuns.WithLabelValues(side, metrics.Result(err)).Inc()
		return res, err
	default:
		return nil, fmt.Errorf("tradeflow: unsupported market side %q", order.MarketSide)
	}
}

func (tf *TradeFlow) requireCapabilities(ctx context.Context, order Order) error {
	if strings.EqualFold(strings.TrimSpace(order.Dex), "mostro") {
		checker, ok := tf.hedgelessDEX.(interface {
			CheckMostro(context.Context, dexaccount.OfferInput) error
		})
		if !ok {
			return fmt.Errorf("mostro preflight is not configured")
		}
		var quote struct {
			Node     string `json:"venue_node_pubkey"`
			Event    string `json:"source_event_id"`
			Protocol int    `json:"protocol_version"`
		}
		if err := json.Unmarshal(order.SelectedOffer, &quote); err != nil {
			return fmt.Errorf("mostro signed quote is required")
		}
		method := order.PaymentMethod
		if method == "" && len(order.PaymentMethods) == 1 {
			method = order.PaymentMethods[0]
		}
		if err := checker.CheckMostro(ctx, dexaccount.OfferInput{OfferID: order.OfferID, FiatAmount: order.FiatAmount, FiatCurrency: order.FiatCurrency, PaymentMethod: method, VenueNodePubKey: quote.Node, SourceEventID: quote.Event, ProtocolVersion: quote.Protocol, MaxPaymentSat: int64(math.Round((order.RequiredBTCLiquidity - order.SecurityDepositBTC) * 1e8)), MaxBondSat: int64(math.Round(order.SecurityDepositBTC * 1e8)), RoutingFeeSat: order.RoutingFeeAllowanceSat}); err != nil {
			return err
		}
	}
	if strings.EqualFold(strings.TrimSpace(order.Dex), "robosats") {
		checker, ok := tf.hedgelessDEX.(interface {
			CheckRoboSats(context.Context, dexaccount.OfferInput) error
		})
		if !ok {
			return fmt.Errorf("RoboSats preflight is not configured")
		}
		id := order.TransactionID
		in := robosatsOfferInput(order, id)
		if strings.TrimSpace(in.CoordinatorID) == "" {
			return fmt.Errorf("RoboSats coordinator identity is required; refresh the offer from source")
		}
		if err := checker.CheckRoboSats(ctx, in); err != nil {
			return err
		}
	}

	if tf.capabilities == nil {
		// Capability enforcement is an injected boundary so isolated unit tests and
		// recovery-only tools do not need a database. Production wiring always sets it.
		return nil
	}
	spot, err := tf.capabilities.State(ctx, order.Provider)
	if err != nil {
		return fmt.Errorf("load spot capability policy: %w", err)
	}
	if err := mmcapabilities.RequireSpot(spot); err != nil {
		return err
	}
	if cexaccount.NormalizeTradeMode(order.TradeMode) == cexaccount.TradeModeHedgelessDoubleCollateral {
		return nil
	}
	provider, ok := mmcapabilities.ProviderForHedgeVenue(order.HedgeVenue)
	if !ok {
		return fmt.Errorf("unknown hedge venue %q", order.HedgeVenue)
	}
	hedge, err := tf.capabilities.State(ctx, provider.ID)
	if err != nil {
		return fmt.Errorf("load hedge capability policy: %w", err)
	}
	return mmcapabilities.RequireHedge(hedge)
}

// runBid executes the bid-side cycle: pre-trade checks → spot buy → hedge → withdraw.
// Each irreversible step is persisted immediately after it completes, so a partial
// cycle is recoverable from the database rather than only existing in this request.
func (tf *TradeFlow) runBid(ctx context.Context, order Order) (*Result, error) {
	slog.Info("tradeflow: starting bid-side cycle",
		"provider", order.Provider,
		"btc_amount", order.BTCAmount,
		"fiat_currency", order.FiatCurrency,
		"hedge_venue", order.HedgeVenue,
	)

	// The route decides where the CEX withdraws to: direct DEXes (bisq v1) fund their
	// own wallet, hub/swap DEXes fund our LND hub.
	marketSide := normalizeMarketSide(order.MarketSide)
	settlement, err := settlementFor(order)
	if err != nil {
		return nil, err
	}

	preTrade := pretrade.Params{
		Provider:                 order.Provider,
		FiatCurrency:             order.FiatCurrency,
		RequiredCapital:          order.RequiredCapital,
		RequiredBTCLiquidity:     order.RequiredBTCLiquidity,
		PaymentMethod:            order.PaymentMethod,
		PaymentMethods:           order.PaymentMethods,
		Dex:                      order.Dex,
		MarketSide:               marketSide,
		Settlement:               order.ResolvedRouteSettlement(),
		TradeMode:                order.TradeMode,
		HedgeVenue:               order.HedgeVenue,
		RoutingFeeAllowanceSat:   routingFeeLimit(order),
		LightningLiquiditySource: order.LightningLiquiditySource,
	}

	// Step 1: pre-trade checks (read-only; result logged for observability).
	report, err := tf.svc.PreTradeCheck(ctx, preTrade)
	if err != nil {
		return nil, apierror.Wrapf(fmt.Errorf("tradeflow: pre-trade check error: %w", err), CodePreTradeFailed,
			"The pre-trade checks couldn't run, so nothing was traded: %s", explain(err)).WithStatus(http.StatusBadGateway)
	}
	if ok, failed := preTradePassed(report, settlement); !ok {
		return nil, apierror.Newf(CodePreTradeFailed,
			"The pre-trade checks failed, so nothing was traded: %s. Fix these and confirm again.", strings.TrimSuffix(strings.Join(failed, "; "), ".")).WithStatus(http.StatusConflict)
	}
	// If the caller did not explicitly pick a payment method, the pre-trade check
	// resolved the highest-priority linked+active catalog entry. Persist that name so
	// chat-venue DEXes can pick the correct template later.
	if strings.TrimSpace(order.PaymentMethod) == "" && strings.TrimSpace(report.ResolvedPaymentMethod) != "" {
		order.PaymentMethod = report.ResolvedPaymentMethod
		preTrade.PaymentMethod = report.ResolvedPaymentMethod
	}

	// target is the address the CEX withdrawal is sent to.
	target, err := tf.resolveWithdrawTarget(ctx, order, settlement, report)
	if err != nil {
		return nil, err
	}
	slog.Info("tradeflow: pre-trade checks passed",
		"provider", order.Provider,
		"route_method", settlement,
		"withdraw_target", target,
	)

	// Persistence is enabled only when the order links a DEX offer and a store is
	// configured; otherwise the flow runs without recording (legacy behaviour).
	persist := tf.store != nil && strings.TrimSpace(order.OfferID) != "" && strings.TrimSpace(order.Dex) != ""

	var txID uint
	if persist {
		txID, err = createTradeflowRow(tf.store, order, target, settlement)
		if err != nil {
			return nil, apierror.Wrap(fmt.Errorf("tradeflow: persist tradeflow start: %w", err), CodeBookkeepingFailed,
				"The trade couldn't be recorded in the database, so nothing was traded. Try again; if it keeps failing, check the Postgres container.").WithStatus(http.StatusInternalServerError)
		}
		slog.Info("tradeflow: transaction row created", "transaction_id", txID, "status", "CexTradeflowStarted")
	}

	// Step 2: spot market buy. Coinbase App does not provide a reliable preflight
	// network-fee quote, so buy the intended withdrawal amount plus the persisted
	// conservative reserve and send only the intended net amount.
	spotAmountBTC := order.BTCAmount
	if order.ResolvedRoute.UsesLightning() && order.RequiredBTCLiquidity > spotAmountBTC {
		spotAmountBTC = order.RequiredBTCLiquidity
	}
	if order.BisqPlan != nil && order.BisqPlan.TotalToPaySats > 0 {
		// Bisq needs principal + seller deposit + trading/mining fees in its wallet.
		spotAmountBTC = dexaccount.SatsToBTC(int64(order.BisqPlan.TotalToPaySats))
	}
	if report.SwapRequired {
		swapOverhead := float64(report.ExpectedLockupSat+report.EstimatedFundingFeeSat-report.SwapShortfallSat) / 1e8
		spotAmountBTC += swapOverhead + order.SwapSafetyHeadroomBTC
	}
	netWithdrawalBTC := spotAmountBTC
	if strings.EqualFold(order.Provider, "coinbase") {
		spotAmountBTC += order.CEXWithdrawalFeeBTC
	} else if report.SwapRequired {
		spotAmountBTC += order.CEXWithdrawalFeeBTC
	}
	spotStart := time.Now()
	spot, err := tf.svc.MarketBuyBTC(ctx, spotAmountBTC, preTrade)
	metrics.OrderDuration.WithLabelValues(order.Provider, "spot").Observe(time.Since(spotStart).Seconds())
	metrics.OrdersPlaced.WithLabelValues(order.Provider, "spot", metrics.Result(err)).Inc()
	if err != nil {
		return nil, spotBuyFailed(order.Provider, fmt.Errorf("tradeflow: spot buy failed: %w", err), err)
	}
	filledBTC := spotAmountBTC
	if isRoboOrder(order) && (spot == nil || spot.Execution == nil || spot.Execution.FilledBTC <= 0) {
		return nil, apierror.Newf(CodeNeedsReconcile,
			"The spot buy on %s returned no confirmed fill, so the RoboSats trade was stopped before anything else moved. Check the order on %s and reconcile it in the Transactions panel before retrying.", order.Provider, order.Provider).
			WithStatus(http.StatusBadGateway).WithLeg(LegSpot)
	}
	if spot.Execution != nil {
		filledBTC = spot.Execution.FilledBTC
	}
	slog.Info("tradeflow: spot buy submitted",
		"order_id", spot.OrderID,
		"status", spot.Status,
		"filled_btc", filledBTC,
	)
	if persist {
		if err := recordSpotBuy(tf.store, txID, spot, filledBTC); err != nil {
			return nil, apierror.Wrapf(fmt.Errorf("tradeflow: persist spot buy (order_id=%s): %w", spot.OrderID, err), CodeBookkeepingFailed,
				"The spot buy on %s went through (order %s), but saving it to the database failed, so the trade was stopped there. Don't retry: the BTC is on %s; record the order in the Transactions panel and check the Postgres container.", order.Provider, spot.OrderID, order.Provider).
				WithStatus(http.StatusInternalServerError).WithLeg(LegSpot)
		}
	}
	if isRoboOrder(order) && filledBTC+1e-8 < spotAmountBTC {
		return nil, apierror.Newf(CodeNeedsReconcile,
			"The spot buy on %s filled only %.8f of the %.8f BTC the RoboSats trade needs, so the trade was stopped. The bought BTC is on %s; reconcile the order in the Transactions panel.", order.Provider, filledBTC, spotAmountBTC, order.Provider).
			WithStatus(http.StatusConflict).WithLeg(LegSpot)
	}
	if strings.EqualFold(order.Provider, "coinbase") && filledBTC+1e-8 < netWithdrawalBTC+order.CEXWithdrawalFeeBTC {
		shortFillErr := fmt.Errorf("coinbase fill %.8f BTC is below net target plus withdrawal reserve %.8f BTC; hedge not opened", filledBTC, netWithdrawalBTC+order.CEXWithdrawalFeeBTC)
		if persist {
			if perr := markHedgeFailed(tf.store, txID, spot, shortFillErr); perr != nil {
				return nil, apierror.Wrapf(fmt.Errorf("tradeflow: persist Coinbase short-fill review state: %w", perr), CodeBookkeepingFailed,
					"Coinbase filled only %.8f BTC, less than needed, and saving that to the database failed. Don't retry: review the Coinbase order (%s) manually and check the Postgres container.", filledBTC, spot.OrderID).
					WithStatus(http.StatusInternalServerError).WithLeg(LegSpot)
			}
		}
		return nil, apierror.Wrapf(fmt.Errorf("tradeflow: %w", shortFillErr), CodeNeedsReconcile,
			"Coinbase filled only %.8f BTC, less than the %.8f BTC needed (trade amount plus withdrawal reserve), so no hedge was opened. The bought BTC is on Coinbase; review the trade in the Transactions panel.", filledBTC, netWithdrawalBTC+order.CEXWithdrawalFeeBTC).
			WithStatus(http.StatusConflict).WithLeg(LegSpot)
	}
	if order.BisqPlan != nil && filledBTC+1e-8 < netWithdrawalBTC {
		shortFillErr := fmt.Errorf("CEX fill %.8f BTC is below Bisq funding requirement %.8f BTC; hedge not opened", filledBTC, netWithdrawalBTC)
		if persist {
			if perr := markHedgeFailed(tf.store, txID, spot, shortFillErr); perr != nil {
				return nil, apierror.Wrapf(fmt.Errorf("tradeflow: persist Bisq short-fill review state: %w", perr), CodeBookkeepingFailed,
					"%s filled only %.8f BTC, less than Bisq needs, and saving that to the database failed. Don't retry: review the order (%s) manually and check the Postgres container.", order.Provider, filledBTC, spot.OrderID).
					WithStatus(http.StatusInternalServerError).WithLeg(LegSpot)
			}
		}
		return nil, apierror.Wrapf(fmt.Errorf("tradeflow: %w", shortFillErr), CodeNeedsReconcile,
			"%s filled only %.8f BTC, less than the %.8f BTC the Bisq trade needs, so no hedge was opened. The bought BTC is on %s; review the trade in the Transactions panel.", order.Provider, filledBTC, netWithdrawalBTC, order.Provider).
			WithStatus(http.StatusConflict).WithLeg(LegSpot)
	}

	// Step 3: open hedge with a guarded retry. From here on the spot is filled, so a
	// hard failure means we hold unhedged BTC — recorded as HedgeFailedNeedsReview.
	hedge, err := tf.openHedgeWithRetry(ctx, order)
	if err != nil {
		metrics.HedgeFailures.WithLabelValues(order.HedgeVenue).Inc()
		if persist {
			if perr := markHedgeFailed(tf.store, txID, spot, err); perr != nil {
				slog.Error("tradeflow: failed to persist HedgeFailedNeedsReview", "transaction_id", txID, "err", perr)
			}
		}
		slog.Error("tradeflow: HEDGE FAILED after spot fill — account holds unhedged BTC, manual review required",
			"transaction_id", txID,
			"spot_order_id", spot.OrderID,
			"filled_btc", filledBTC,
			"hedge_venue", order.HedgeVenue,
			"err", err,
		)
		return nil, apierror.Wrapf(fmt.Errorf("tradeflow: hedge open failed (spot filled, order_id=%s, needs review): %w", spot.OrderID, err), CodeHedgeFailed,
			"Opening the hedge on %s failed after the spot buy filled (order %s), so %.8f BTC is now unhedged. Hedge or sell it manually, then mark the trade reviewed in the Transactions panel. Reason: %s", order.HedgeVenue, spot.OrderID, filledBTC, explain(err)).
			WithStatus(http.StatusBadGateway).WithLeg(LegHedge)
	}
	slog.Info("tradeflow: hedge opened",
		"venue", hedge.Venue,
		"order_id", hedge.OrderID,
		"status", hedge.Status,
	)
	if persist {
		if err := recordHedge(tf.store, txID, order, spot, hedge); err != nil {
			return nil, apierror.Wrapf(fmt.Errorf("tradeflow: persist hedge: %w", err), CodeBookkeepingFailed,
				"The hedge on %s was opened (order %s), but saving it to the database failed, so the withdrawal was not started. Don't retry: record the trade in the Transactions panel and check the Postgres container.", hedge.Venue, hedge.OrderID).
				WithStatus(http.StatusInternalServerError).WithLeg(LegHedge)
		}
	}

	// Step 4: withdraw the net filled BTC to the LND address resolved during pre-trade
	// checks. The client request id is derived from the transaction id so a retry is
	// idempotent at the exchange (no double withdrawal).
	clientReqID := ""
	if txID > 0 {
		clientReqID = fmt.Sprintf("tradeflow-%d", txID)
	}
	wdStart := time.Now()
	withdrawAmountBTC := filledBTC
	if strings.EqualFold(order.Provider, "coinbase") || order.BisqPlan != nil {
		withdrawAmountBTC = netWithdrawalBTC
	}
	wd, err := tf.svc.WithdrawSpotToLND(ctx, order.Provider, withdrawAmountBTC, target, clientReqID)
	metrics.OrderDuration.WithLabelValues(order.Provider, "withdrawal").Observe(time.Since(wdStart).Seconds())
	metrics.OrdersPlaced.WithLabelValues(order.Provider, "withdrawal", metrics.Result(err)).Inc()
	if err != nil {
		return nil, apierror.Wrapf(fmt.Errorf("tradeflow: withdrawal failed (spot order_id=%s, hedge open): %w", spot.OrderID, err), CodeWithdrawalFailed,
			"Withdrawing %.8f BTC from %s to the Lightning node failed after the spot buy (order %s) and the hedge went through. The BTC is still on %s; withdraw it manually, then reconcile the trade in the Transactions panel. Reason: %s", withdrawAmountBTC, order.Provider, spot.OrderID, order.Provider, explain(err)).
			WithStatus(http.StatusBadGateway).WithLeg(LegWithdrawal)
	}
	slog.Info("tradeflow: withdrawal initiated",
		"withdrawal_id", wd.WithdrawalID,
		"wallet_address", wd.WalletAddress,
		"amount_btc", withdrawAmountBTC,
	)

	result := &Result{
		SpotBuy:   spot,
		Hedge:     hedge,
		TradeMode: cexaccount.TradeModeHedgefullSingleCollateral,
		Withdrawal: &WithdrawalResult{
			Provider:        order.Provider,
			WithdrawalID:    wd.WithdrawalID,
			ClientRequestID: wd.ClientRequestID,
			WalletAddress:   wd.WalletAddress,
			AmountBTC:       withdrawAmountBTC,
			CexStatus:       "submitted",
		},
		TransactionID: txID,
	}

	if persist {
		if err := recordWithdrawal(tf.store, txID, order, spot, hedge, result, target, settlement); err != nil {
			return nil, apierror.Wrapf(fmt.Errorf("tradeflow: persist withdrawal: %w", err), CodeBookkeepingFailed,
				"The withdrawal from %s was submitted (ID %s), but saving it to the database failed. Don't retry: record the trade in the Transactions panel and check the Postgres container.", order.Provider, wd.WithdrawalID).
				WithStatus(http.StatusInternalServerError).WithLeg(LegWithdrawal)
		}
	}

	return result, nil
}

// ResolvedRouteSettlement returns the immutable Unified API settlement method.
// Empty is reserved for the legacy compatibility path.
func (o Order) ResolvedRouteSettlement() string {
	if o.ResolvedRoute == nil {
		return ""
	}
	return o.ResolvedRoute.Settlement
}

// resolvedMethod converts the already-selected Unified API method into the
// controller's execution enum. It intentionally does not inspect the DEX.
func settlementFor(order Order) (string, error) {
	if order.ResolvedRoute == nil || !order.ResolvedRoute.ValidForExecution() {
		return "", fmt.Errorf("tradeflow: resolved_route is required")
	}
	return order.ResolvedRoute.Settlement, nil
}

func normalizeMarketSide(raw string) string {
	if strings.EqualFold(strings.TrimSpace(raw), "ask") {
		return "ask"
	}
	return "bid"
}

// preTradePassed reports whether the pre-trade checks allow the trade to proceed,
// returning the messages of any blocking failures. The "whitelist" check verifies that
// an LND hub address is whitelisted at the CEX — only relevant for hub/swap routes; the
// direct route withdraws to the bisq wallet instead, so its failure is not blocking.
func preTradePassed(report *pretrade.Report, settlement string) (bool, []string) {
	var failed []string
	for _, chk := range report.Checks {
		if chk.Passed {
			continue
		}
		if settlement == "onchain_direct" && chk.Name == "whitelist" {
			continue
		}
		failed = append(failed, chk.Message)
	}
	return len(failed) == 0, failed
}

// resolveWithdrawTarget returns the address the CEX withdrawal is sent to: the bisq
// wallet's funding address for the direct route, or the whitelisted LND hub address for
// the hub/swap routes.
func (tf *TradeFlow) resolveWithdrawTarget(ctx context.Context, order Order, settlement string, report *pretrade.Report) (string, error) {
	if settlement == "onchain_direct" {
		if order.BisqPlan != nil && strings.TrimSpace(order.BisqPlan.FundingAddress) != "" {
			return strings.TrimSpace(order.BisqPlan.FundingAddress), nil
		}
		if tf.directResolver == nil {
			return "", fmt.Errorf("tradeflow: direct route (%s) requires a DEX wallet resolver, none configured", order.Dex)
		}
		addr, err := tf.directResolver.DirectDestination(ctx, order.Dex)
		if err != nil {
			return "", fmt.Errorf("tradeflow: resolve direct destination for %s: %w", order.Dex, err)
		}
		if strings.TrimSpace(addr) == "" {
			return "", fmt.Errorf("tradeflow: direct route resolved an empty destination address for %s", order.Dex)
		}
		return strings.TrimSpace(addr), nil
	}

	addr := strings.TrimSpace(report.LNDWithdrawAddress)
	if addr == "" {
		return "", fmt.Errorf("tradeflow: pre-trade checks passed but no LND withdrawal address was resolved")
	}
	return addr, nil
}

// openHedgeWithRetry opens the hedge, retrying on error. Before each retry it asks
// the venue whether the failed attempt actually opened the position (a lost
// confirmation can make a successful open look like a failure); if so it stops
// rather than opening a second, doubling-up hedge.
func (tf *TradeFlow) openHedgeWithRetry(ctx context.Context, order Order) (*cexaccount.HedgeOpenResult, error) {
	if isRoboOrder(order) {
		// Without a portable, persisted client-order id, an error may hide a
		// delayed hedge fill. The durable worker parks this outcome; no retry.
		hedge, err := tf.svc.OpenHedge(ctx, order.HedgeVenue, order.HedgeSide, order.HedgeNotionalBTC, order.Leverage)
		if err != nil {
			return nil, err
		}
		if hedge == nil {
			return nil, fmt.Errorf("RoboSats hedge returned no receipt")
		}
		return hedge, nil
	}
	side := strings.ToLower(strings.TrimSpace(order.HedgeSide))

	// Baseline: how much is already open on the expected side before we try. A
	// fresh trade is normally flat, but this also tolerates pre-existing positions.
	baseline := tf.sideSizeBTC(ctx, order.HedgeVenue, side)

	var lastErr error
	for attempt := 1; attempt <= hedgeMaxAttempts; attempt++ {
		attemptStart := time.Now()
		hedge, err := tf.svc.OpenHedge(ctx, order.HedgeVenue, order.HedgeSide, order.HedgeNotionalBTC, order.Leverage)
		metrics.OrderDuration.WithLabelValues(order.HedgeVenue, "hedge").Observe(time.Since(attemptStart).Seconds())
		metrics.OrdersPlaced.WithLabelValues(order.HedgeVenue, "hedge", metrics.Result(err)).Inc()
		if err == nil {
			return hedge, nil
		}
		lastErr = err
		slog.Warn("tradeflow: hedge attempt failed",
			"attempt", attempt,
			"venue", order.HedgeVenue,
			"err", err,
		)

		// Did the attempt actually open despite the error? Query the venue directly.
		snap, perr := tf.svc.LiveHedgePosition(ctx, order.HedgeVenue)
		if perr != nil {
			slog.Warn("tradeflow: live hedge position check failed; cannot confirm fill",
				"venue", order.HedgeVenue, "err", perr)
		} else if snap.Side == side && (snap.SizeBTC-baseline) >= order.HedgeNotionalBTC*hedgeOpenedFraction {
			slog.Warn("tradeflow: hedge call errored but venue shows the position is open; treating as success",
				"venue", order.HedgeVenue,
				"live_size_btc", snap.SizeBTC,
				"baseline_btc", baseline,
				"expected_btc", order.HedgeNotionalBTC,
			)
			return &cexaccount.HedgeOpenResult{
				Venue:       order.HedgeVenue,
				Side:        side,
				BTCNotional: snap.SizeBTC - baseline,
				Status:      "open_detected_after_error",
			}, nil
		}

		if attempt < hedgeMaxAttempts {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(hedgeRetryDelay):
			}
		}
	}
	return nil, fmt.Errorf("hedge open failed after %d attempts: %w", hedgeMaxAttempts, lastErr)
}

// sideSizeBTC returns the live position size on the given side, or 0 if flat, the
// side differs, or the venue query fails (best-effort baseline).
func (tf *TradeFlow) sideSizeBTC(ctx context.Context, venue, side string) float64 {
	snap, err := tf.svc.LiveHedgePosition(ctx, venue)
	if err != nil || snap == nil || snap.Side != side {
		return 0
	}
	return snap.SizeBTC
}

// runAsk will execute the ask-side cycle (sell BTC on CEX, long hedge, deposit to CEX).
// Planned pre-trade checks: API key permissions, sufficient CEX capital for the long
// hedge (and security deposit when the offer requires one). Whitelist/LND withdrawal
// checks are not needed on the ask path.
func (tf *TradeFlow) runAsk(_ context.Context, _ Order) (*Result, error) {
	return nil, fmt.Errorf("tradeflow: ask-side trade cycle is not implemented yet")
}

func validate(o Order) error {
	if strings.EqualFold(strings.TrimSpace(o.Dex), "mostro") && (strings.ToLower(strings.TrimSpace(o.MarketSide)) != "bid" || o.ResolvedRoute == nil || !o.ResolvedRoute.UsesLightning()) {
		return ErrMostroRoute()
	}

	if o.ResolvedRoute != nil && !o.ResolvedRoute.ValidForExecution() {
		return RouteProblem("tradeflow: invalid resolved_route")
	}
	side := strings.ToLower(strings.TrimSpace(o.MarketSide))
	if side != "bid" && side != "ask" {
		return RouteProblem("tradeflow: market_side must be \"bid\" or \"ask\", got %q", o.MarketSide)
	}
	if strings.TrimSpace(o.Provider) == "" {
		return RouteProblem("tradeflow: provider is required")
	}
	if o.BTCAmount <= 0 {
		return RouteProblem("tradeflow: btc_amount must be greater than zero")
	}
	if o.RequiredCapital <= 0 {
		return RouteProblem("tradeflow: required_capital must be greater than zero")
	}
	if o.RoutingFeeAllowanceSat < 0 || o.CEXWithdrawalFeeBTC < 0 || o.SwapSafetyHeadroomBTC < 0 {
		return RouteProblem("tradeflow: swap fee allowances and safety headroom cannot be negative")
	}
	mode := cexaccount.NormalizeTradeMode(o.TradeMode)
	if o.ResolvedRoute != nil && o.ResolvedRoute.UsesLightning() && o.RequiredBTCLiquidity <= 0 {
		return RouteProblem("tradeflow: required_btc_liquidity must be greater than zero for Swap_main2lightning routes")
	}
	if mode == cexaccount.TradeModeHedgelessDoubleCollateral {
		// Hedgeless execution intentionally has a narrow support matrix. Rejecting
		// unsupported sides/venues/providers here prevents a partially completed
		// money flow from discovering the limitation after BTC has moved.
		if side != "bid" {
			return ErrHedgelessAskSide()
		}
		if o.RequiredBTCLiquidity <= 0 {
			return RouteProblem("tradeflow: required_btc_liquidity must be greater than zero in hedgeless mode")
		}
		if o.SecurityDepositBTC < 0 || o.RequiredBTCLiquidity+1e-12 < o.BTCAmount+o.SecurityDepositBTC {
			return RouteProblem("tradeflow: required_btc_liquidity must cover btc_amount plus security_deposit_btc")
		}
		if strings.TrimSpace(o.OfferID) == "" || strings.TrimSpace(o.Dex) == "" {
			return RouteProblem("tradeflow: offer_id and dex are required in hedgeless mode")
		}
		if o.FiatAmount <= 0 {
			return RouteProblem("tradeflow: fiat_amount must be greater than zero in hedgeless mode")
		}
		switch strings.ToLower(strings.TrimSpace(o.Dex)) {
		case "bisq", "hodlhodl", "robosats":
		default:
			return ErrHedgelessDEX(o.Dex)
		}
		switch strings.ToLower(strings.TrimSpace(o.Provider)) {
		case "binance", "kraken", "coinbase":
		default:
			return ErrHedgelessProvider(o.Provider, "Binance", "Kraken", "Coinbase")
		}
	} else {
		if strings.TrimSpace(o.HedgeVenue) == "" {
			return ErrNoHedgeVenue()
		}
		if strings.TrimSpace(o.HedgeSide) == "" {
			return RouteProblem("tradeflow: hedge_side is required")
		}
		if o.HedgeNotionalBTC <= 0 {
			return RouteProblem("tradeflow: hedge_notional_btc must be greater than zero")
		}
	}
	return nil
}

func robosatsOfferInput(order Order, txID uint) dexaccount.OfferInput {
	var selected struct {
		Terms *robosats.NativeTerms `json:"native_terms"`
	}
	_ = json.Unmarshal(order.SelectedOffer, &selected)
	method := strings.TrimSpace(order.PaymentMethod)
	if method == "" && len(order.PaymentMethods) == 1 {
		method = order.PaymentMethods[0]
	}
	maxBond := int64FromOfferJSON(order.SelectedOffer, "max_bond_sat")
	if maxBond < 0 {
		maxBond = 0
	}
	if maxBond == 0 {
		maxBond = dexaccount.BTCToSats(order.SecurityDepositBTC)
	}
	maxEscrow := int64FromOfferJSON(order.SelectedOffer, "max_escrow_sat")
	if maxEscrow <= 0 {
		maxEscrow = dexaccount.BTCToSats(order.RequiredBTCLiquidity - order.SecurityDepositBTC)
		if maxEscrow <= 0 {
			maxEscrow = dexaccount.BTCToSats(order.BTCAmount)
		}
	}
	return dexaccount.OfferInput{
		Exchange:        strings.TrimSpace(order.Dex),
		OfferID:         strings.TrimSpace(order.OfferID),
		BTCAmount:       order.BTCAmount,
		FiatAmount:      order.FiatAmount,
		RequestID:       uint64(txID),
		VenueNodePubKey: strings.TrimSpace(stringFromOfferJSON(order.SelectedOffer, "venue_node_pubkey")),
		ProtocolVersion: int(int64FromOfferJSON(order.SelectedOffer, "protocol_version")),
		SourceEventID:   strings.TrimSpace(stringFromOfferJSON(order.SelectedOffer, "source_event_id")),
		FiatCurrency:    order.FiatCurrency,
		PaymentMethod:   method,
		MaxPaymentSat:   maxEscrow,
		MaxBondSat:      maxBond,
		RoutingFeeSat:   routingFeeLimit(order),
		CoordinatorID:   strings.TrimSpace(stringFromOfferJSON(order.SelectedOffer, "venue_coordinator_id")),
		QuoteID:         firstNonEmpty(order.QuoteID, stringFromOfferJSON(order.SelectedOffer, "quote_id")),
		NativeTerms:     selected.Terms,
		BondFeeSat:      int64FromOfferJSON(order.SelectedOffer, "bond_fee_sat"),
		EscrowFeeSat:    int64FromOfferJSON(order.SelectedOffer, "escrow_fee_sat"),
	}
}

func int64FromOfferJSON(raw []byte, field string) int64 {
	var offer map[string]any
	if json.Unmarshal(raw, &offer) != nil {
		return 0
	}
	switch value := offer[field].(type) {
	case float64:
		return int64(value)
	case json.Number:
		n, _ := value.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return n
	}
	return 0
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
