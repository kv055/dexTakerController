package tradeflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"dexTakerController/internal/cexaccount"
	"dexTakerController/internal/db"
	"dexTakerController/internal/dexaccount"
	"dexTakerController/internal/exchangeconnectors/dex/robosats"
)

type roboJobStore interface {
	AdmitRoboSats(context.Context, string, json.RawMessage, *robosats.Session) (uint, error)
	LoadExecutionQuote(context.Context, string) (*db.ExecutionQuote, error)
	RoboSatsJobs(context.Context) ([]db.RoboSatsJob, error)
	SetRoboSatsJob(context.Context, uint, string, string) error
	LockRoboSatsExecution(context.Context, uint) (func(), error)
}

type roboPreparer interface {
	PrepareRoboSats(context.Context, dexaccount.OfferInput) (*robosats.Session, error)
	QuoteRoboSats(context.Context, robosats.QuoteInput) (*robosats.NativeTerms, error)
}

// WithRoboSats installs one durable owner for both funding modes. Admission is
// disabled unless its worker is enabled; recovery is independent of the new-trade gate.
func (tf *TradeFlow) WithRoboSats(c *robosats.Coordinator, workerEnabled bool, relay, chat func(context.Context, *db.Transaction) error) *TradeFlow {
	tf.robo, tf.roboWorkerEnabled, tf.roboRelay, tf.roboChat = c, workerEnabled, relay, chat
	return tf
}

func (tf *TradeFlow) QuoteRoboSats(ctx context.Context, input robosats.QuoteInput) (*robosats.NativeTerms, error) {
	p, ok := tf.hedgelessDEX.(roboPreparer)
	if !ok || !tf.roboWorkerEnabled {
		return nil, robosats.ErrDisabled()
	}
	return p.QuoteRoboSats(ctx, input)
}

func (tf *TradeFlow) AdmitRoboSats(ctx context.Context, order Order) (uint, error) {
	repo, ok := tf.store.(roboJobStore)
	if !ok {
		return 0, fmt.Errorf("RoboSats durable repository is unavailable")
	}
	// Check binding before any live preflight: a retry of an admitted quote only
	// returns its transaction, even if the robot is now busy or execution disabled.
	q, err := repo.LoadExecutionQuote(ctx, order.QuoteID)
	if err != nil {
		return 0, err
	}
	if q.TransactionID > 0 {
		return q.TransactionID, nil
	}
	if !tf.roboWorkerEnabled || tf.robo == nil || !tf.robo.Enabled {
		return 0, robosats.ErrDisabled()
	}
	if order.Dex != "robosats" || order.MarketSide != "bid" || order.TransactionID != 0 || q.Exchange != "robosats" || q.OfferID != order.OfferID || q.Route == nil {
		return 0, RouteProblem("RoboSats admission requires an unbound bid quote (dex=%q side=%q tx=%d quote_exchange=%q quote_offer=%q offer=%q has_route=%t)",
			order.Dex, order.MarketSide, order.TransactionID, q.Exchange, q.OfferID, order.OfferID, q.Route != nil)
	}
	if err := validate(order); err != nil {
		return 0, err
	}
	if err := tf.requireCapabilities(ctx, order); err != nil {
		return 0, err
	}
	p, ok := tf.hedgelessDEX.(roboPreparer)
	if !ok {
		return 0, robosats.ErrDisabled()
	}
	s, err := p.PrepareRoboSats(ctx, robosatsOfferInput(order, 0))
	if err != nil {
		return 0, err
	}
	raw, err := json.Marshal(order)
	if err != nil {
		return 0, err
	}
	return repo.AdmitRoboSats(ctx, q.ID, raw, s)
}

func (tf *TradeFlow) RunRoboSatsWorker(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		tf.runRoboSatsOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (tf *TradeFlow) runRoboSatsOnce(ctx context.Context) {
	repo, ok := tf.store.(roboJobStore)
	if !ok || tf.robo == nil {
		return
	}
	jobs, err := repo.RoboSatsJobs(ctx)
	if err != nil {
		slog.Error("RoboSats jobs unavailable", "err", err)
		return
	}
	for _, job := range jobs {
		if ctx.Err() != nil {
			return
		}
		// A bounded pass survives HTTP disconnects, yet yields regularly for
		// shutdown and restarts. Coordinator payments recover by hash next pass.
		pass, cancel := context.WithTimeout(ctx, 45*time.Second)
		err := tf.runRoboSatsJob(pass, repo, job)
		cancel()
		if err != nil {
			slog.Warn("RoboSats execution waiting", "transaction_id", job.TransactionID, "err", err)
		}
	}
}

func (tf *TradeFlow) runRoboSatsJob(ctx context.Context, repo roboJobStore, job db.RoboSatsJob) error {
	unlock, err := repo.LockRoboSatsExecution(ctx, job.TransactionID)
	if err != nil {
		return err
	}
	defer unlock()
	// Reload under the lock: another process may have finished a queued job
	// after this process listed it. Never use the stale queue snapshot to run it.
	jobs, err := repo.RoboSatsJobs(ctx)
	if err != nil {
		return err
	}
	found := false
	for _, current := range jobs {
		if current.TransactionID == job.TransactionID {
			job, found = current, true
			break
		}
	}
	if !found {
		return nil
	}
	var order Order
	if err := json.Unmarshal(job.OrderData, &order); err != nil {
		return err
	}
	order.TransactionID = job.TransactionID
	tx, err := tf.store.GetTransaction(job.TransactionID)
	if err != nil {
		return err
	}
	if tx.TransactionComplete {
		return tf.completeRoboSats(ctx, tx)
	}
	park := func(reason string) error {
		if err := mutateRow(tf.store, tx.ID, func(t *db.Transaction) { t.SetStatus(db.StatusTransactionNeedsReview, reason) }); err != nil {
			return err
		}
		return repo.SetRoboSatsJob(ctx, tx.ID, "needs_review", reason)
	}
	if job.State == "queued" {
		if err := repo.SetRoboSatsJob(ctx, tx.ID, "running", ""); err != nil {
			return err
		}
		_, err = tf.Run(ctx, order)
	} else if job.State != "needs_review" {
		switch tx.Status {
		case db.StatusTransactionNeedsReview:
			return park("worker stopped before preparation completed; inspect before restarting")
		case db.StatusHedgelessTradeStarted, db.StatusHedgelessFundingInitiated:
			session, loadErr := tf.robo.Progress(ctx, uint64(tx.ID))
			if loadErr != nil {
				return loadErr
			}
			if session == nil || !session.PreparationComplete {
				return park("liquidity preparation was interrupted; reconcile its outcome before resuming")
			}
			_, err = tf.resumeRoboSatsHedgeless(ctx, tx.ID, order)
		case db.StatusCexTradeflowStarted, db.StatusHedgeFailedNeedsReview, db.StatusCexTradeSuccessful:
			// A CEX call may have succeeded without a local receipt. Existing
			// cross-venue recovery cannot prove absence of a delayed fill. Do not
			// re-buy, re-hedge or re-withdraw automatically in this ambiguity.
			return park("CEX operation lacks a complete receipt; reconcile with exchange before resuming")
		case db.StatusCexCryptoWithdrawalInitiated, db.StatusCexCryptoWithdrawalArrived:
			if tf.roboRelay == nil {
				return fmt.Errorf("RoboSats relay unavailable")
			}
			err = tf.roboRelay(ctx, tx)
		}
	}
	// Persist diagnostics with a fresh context even when this pass timed out.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	s, loadErr := tf.robo.Progress(cleanup, uint64(tx.ID))
	if loadErr != nil {
		return loadErr
	}
	if s == nil {
		return fmt.Errorf("RoboSats admitted identity is missing")
	}
	if s.Phase == robosats.PhaseNeedsReview {
		if e := repo.SetRoboSatsJob(cleanup, tx.ID, "needs_review", s.LastError); e != nil {
			return e
		}
		if e := mutateRow(tf.store, tx.ID, func(t *db.Transaction) { t.SetStatus(db.StatusDexTakenFundingNeedsReview, s.LastError) }); e != nil {
			return e
		}
	}
	if err != nil && s.Phase != robosats.PhaseNeedsReview {
		_ = repo.SetRoboSatsJob(cleanup, tx.ID, "running", err.Error())
	}
	if s.Funded {
		fresh, e := tf.store.GetTransaction(tx.ID)
		if e != nil {
			return e
		}
		// Chat failure must never undo escrow confirmation or restart funding.
		if tf.roboChat != nil && ctx.Err() == nil {
			if e = tf.roboChat(ctx, fresh); e != nil {
				slog.Warn("RoboSats chat pending", "transaction_id", tx.ID, "err", e)
			}
		}
		if fresh.TradeMode == cexaccount.TradeModeHedgelessDoubleCollateral && ctx.Err() == nil {
			if e = tf.advanceRefill(ctx, fresh); e != nil {
				return e
			}
		}
	}
	return err
}

func (tf *TradeFlow) markRoboSatsPrepared(ctx context.Context, id uint) error {
	if tf.robo == nil {
		return fmt.Errorf("RoboSats coordinator unavailable")
	}
	unlock, err := tf.robo.Store.LockRoboSats(ctx, uint64(id))
	if err != nil {
		return err
	}
	defer unlock()
	s, err := tf.robo.Progress(ctx, uint64(id))
	if err != nil {
		return err
	}
	if s == nil {
		return fmt.Errorf("RoboSats session unavailable")
	}
	s.PreparationComplete = true
	return tf.robo.Store.SaveRoboSatsSession(ctx, s)
}

func (tf *TradeFlow) resumeRoboSatsHedgeless(ctx context.Context, id uint, order Order) (*Result, error) {
	if tf.robo == nil {
		return nil, fmt.Errorf("RoboSats coordinator unavailable")
	}
	s, err := tf.robo.Progress(ctx, uint64(id))
	if err != nil {
		return nil, err
	}
	if s == nil || !s.PreparationComplete {
		return nil, fmt.Errorf("RoboSats liquidity preparation needs reconciliation")
	}
	taken, err := tf.hedgelessDEX.Take(ctx, robosatsOfferInput(order, id))
	if err != nil {
		return nil, err
	}
	if taken == nil || taken.Execution["robosats_funded"] != true {
		return nil, fmt.Errorf("RoboSats escrow not confirmed")
	}
	s, err = tf.robo.Progress(ctx, uint64(id))
	if err != nil {
		return nil, err
	}
	if s == nil || s.Payments["escrow"] == nil {
		return nil, fmt.Errorf("RoboSats escrow receipt missing")
	}
	p := s.Payments["escrow"]
	funding := &DEXFundingResult{Exchange: "robosats", Method: "lightning", AmountBTC: float64(p.AmountSat) / 1e8, PaymentHash: p.Hash, Status: "funded"}
	// Commit the DEX receipt before any refill intent. A crash here resumes the
	// refill state, never the original CEX tradeflow or a second DEX payment.
	if err := recordHedgelessDEXTaken(tf.store, id, order, funding, taken.Execution); err != nil {
		return nil, err
	}
	tx, err := tf.store.GetTransaction(id)
	if err != nil {
		return nil, err
	}
	if err := tf.advanceRefill(ctx, tx); err != nil {
		return nil, err
	}
	return &Result{TradeMode: order.TradeMode, TransactionID: id, DEXFunding: funding}, nil
}

func isRoboOrder(order Order) bool {
	return order.QuoteID != "" && strings.EqualFold(order.Dex, "robosats")
}

func (tf *TradeFlow) completeRoboSats(ctx context.Context, tx *db.Transaction) error {
	if tx.TradeMode == cexaccount.TradeModeHedgelessDoubleCollateral && tx.BTCPoolRefillStatus != refillRestored {
		return tf.advanceRefill(ctx, tx)
	}
	s, err := tf.robo.Progress(ctx, uint64(tx.ID))
	if err != nil {
		return err
	}
	if s == nil {
		return fmt.Errorf("RoboSats completion has no session")
	}
	api, err := tf.robo.SessionAPI(ctx, s)
	if err != nil {
		return err
	}
	robot, err := api.GetRobot(ctx)
	if err != nil {
		return err
	}
	active := fmt.Sprint(robot["active_order_id"])
	if active != "<nil>" && active != "" && active != "0" {
		return fmt.Errorf("RoboSats robot still has active order #%s; the trade completes once that order is finished or cancelled in RoboSats", active)
	}
	for _, payment := range s.Payments {
		if !payment.Attempted && !payment.Submitted {
			continue
		}
		status, err := tf.robo.Wallet.TrackHoldPayment(ctx, payment.Hash)
		if err != nil {
			return err
		}
		if status != "SUCCEEDED" && status != "FAILED" {
			return fmt.Errorf("RoboSats hold has not resolved")
		}
	}
	if repo, ok := tf.store.(interface {
		CompleteRoboSats(context.Context, uint) error
	}); ok {
		return repo.CompleteRoboSats(ctx, tx.ID)
	}
	return fmt.Errorf("RoboSats completion repository unavailable")
}
