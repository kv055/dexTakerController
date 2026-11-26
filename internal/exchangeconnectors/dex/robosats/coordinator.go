package robosats

import (
	"commons/apierror"
	"commons/credentialcrypto"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Coordinator runs the durable bond → escrow → chat lifecycle for one trade.
type Coordinator struct {
	Enabled    bool
	Store      Store
	Wallet     Wallet
	Registry   map[string]string
	Dial       func(coordinatorID, token string) (OrderAPI, error)
	DialPinned func(url, token string) (OrderAPI, error)
	Token      func(ctx context.Context, credentialID int64) (string, error)
	Clock      func() time.Time
	Sleep      func(context.Context, time.Duration) error
}

func (c *Coordinator) now() time.Time {
	if c != nil && c.Clock != nil {
		return c.Clock()
	}
	return time.Now()
}

func (c *Coordinator) sleep(ctx context.Context, d time.Duration) error {
	if c != nil && c.Sleep != nil {
		return c.Sleep(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Coordinator) Ready() error {
	if c == nil || c.Store == nil || c.Wallet == nil || c.Dial == nil || c.Token == nil {
		return fmt.Errorf("robosats: durable store, hold-payment wallet and coordinator dialer are required")
	}
	return nil
}

func (c *Coordinator) Check(ctx context.Context, req TakeRequest) error {
	// A read-only check precedes allocation of the durable transaction ID.
	if req.RequestID == 0 {
		req.RequestID = 1
	}
	if c == nil || !c.Enabled {
		return ErrDisabled()
	}
	if err := c.Ready(); err != nil {
		return apierror.Wrap(err, CodeDisabled, "RoboSats trading is not fully set up on this server (the durable store, Lightning wallet or coordinator connection is missing). Check the Taker Controller's RoboSats configuration and restart it.").WithStatus(http.StatusServiceUnavailable)
	}
	if err := validateTakeRequest(req); err != nil {
		return err
	}
	if _, err := c.resolveURL(req.CoordinatorID); err != nil {
		return ErrCoordinatorUnknown(req.CoordinatorID, err)
	}
	token, err := c.Token(ctx, req.CredentialID)
	if err != nil || strings.TrimSpace(token) == "" {
		if err != nil && req.CredentialID != 0 {
			return apierror.Wrap(err, CodeNoRobot, "The RoboSats robot this trade was quoted with is no longer active. Re-activate it under Trading Accounts, or request a new quote.").WithStatus(http.StatusConflict)
		}
		return ErrNoRobot()
	}
	if req.TokenFingerprint != "" && tokenFingerprint(token) != req.TokenFingerprint {
		return apierror.New(CodeRobotRejected, "The RoboSats robot token changed after this trade was quoted, so the trade can't continue with a different robot. Request a new quote.").WithStatus(http.StatusConflict)
	}
	api, err := c.Dial(req.CoordinatorID, token)
	if err != nil {
		return ErrCoordinatorUnknown(req.CoordinatorID, err)
	}
	robot, err := api.GetRobot(ctx)
	if err != nil {
		return explainCoordinatorError(err, "load your robot")
	}
	if active := robotString(robot, "active_order_id"); active != "" && active != req.OrderID {
		return robotBusy(active)
	}
	pos, err := api.GetPosition(ctx, req.OrderID)
	if err != nil {
		return explainCoordinatorError(err, "load this offer")
	}
	if err := offerStateProblem(pos, req.OrderID); err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(pos.PaymentMethod), strings.TrimSpace(req.PaymentMethod)) && req.PaymentMethod != "" {
		if !strings.Contains(strings.ToLower(pos.PaymentMethod), strings.ToLower(req.PaymentMethod)) {
			return apierror.Newf(CodePaymentMethod, "Offer #%s doesn't accept %q on RoboSats (it lists %q). Pick one of the offer's payment methods.", req.OrderID, req.PaymentMethod, strings.TrimSpace(pos.PaymentMethod)).WithStatus(http.StatusConflict)
		}
	}
	if req.Terms != nil {
		return req.Terms.CheckPosition(pos, c.now())
	}
	if req.QuoteID != "" {
		return apierror.New(CodeInvalidTerms, "This RoboSats quote has no verified terms from the coordinator. Close the confirmation and request a new quote.").WithStatus(http.StatusConflict)
	}
	return nil
}

func (c *Coordinator) Take(ctx context.Context, req TakeRequest) (*TakeResult, error) {
	if err := c.Ready(); err != nil {
		return nil, err
	}
	if err := validateTakeRequest(req); err != nil {
		return nil, err
	}
	unlock, err := c.Store.LockRoboSats(ctx, req.RequestID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	s, err := c.Store.LoadRoboSatsSession(ctx, req.RequestID)
	if err != nil {
		return nil, err
	}
	if s == nil {
		if !c.Enabled {
			return nil, ErrDisabled()
		}
		req.TokenFingerprint = firstNonEmpty(req.TokenFingerprint, func() string {
			token, terr := c.Token(ctx, req.CredentialID)
			if terr != nil {
				return ""
			}
			return tokenFingerprint(token)
		}())
		s, err = c.Store.CreateRoboSatsSession(ctx, &Session{
			Request:   req,
			Phase:     PhaseCreated,
			Payments:  map[string]*Payment{},
			CreatedAt: c.now().UTC(),
		})
		if err != nil {
			return nil, err
		}
	} else if !sameTakeRequest(req, s.Request) {
		return nil, fmt.Errorf("robosats: retry changed the original trade terms")
	}
	if s.Phase == PhaseNeedsReview {
		return nil, fmt.Errorf("robosats: trade requires review: %s", s.LastError)
	}
	if err := c.run(ctx, s); err != nil {
		if IsPermanent(err) {
			return nil, c.review(ctx, s, err)
		}
		return nil, err
	}
	return s.takeResult(), nil
}

func (c *Coordinator) Progress(ctx context.Context, requestID uint64) (*Session, error) {
	if c == nil || c.Store == nil {
		return nil, fmt.Errorf("robosats: store is not configured")
	}
	return c.Store.LoadRoboSatsSession(ctx, requestID)
}

func (c *Coordinator) run(ctx context.Context, s *Session) error {
	if s.Funded {
		return nil
	}
	api, err := c.SessionAPI(ctx, s)
	if err != nil {
		return err
	}
	if _, err := api.GetRobot(ctx); err != nil {
		return err
	}
	if id := api.Identity(); id != nil {
		s.PublicKey = id.PublicKeyArmored
		s.EncPrivKey = id.EncPrivKeyArmored
	}
	if err := c.Store.SaveRoboSatsSession(ctx, s); err != nil {
		return err
	}

	if s.Phase == PhaseCreated || s.Phase == PhaseTaking {
		if err := c.takeOrder(ctx, s, api); err != nil {
			return err
		}
	}
	if err := c.advancePayments(ctx, s, api); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) takeOrder(ctx context.Context, s *Session, api OrderAPI) error {
	// Reconcile before repeating an ambiguous take. Never infer absence from an outage.
	if s.Phase == PhaseTaking {
		existing, err := api.GetPosition(ctx, s.Request.OrderID)
		if err != nil {
			return err
		}
		if existing.ID != s.Request.OrderID || !existing.IsTaker || !existing.IsSeller {
			return c.review(ctx, s, permanent("robosats: previous take outcome requires reconciliation"))
		}
		s.RobotNick = existing.UrNick
		s.Phase = PhaseTaken
		return c.Store.SaveRoboSatsSession(ctx, s)
	}
	if s.Request.Terms != nil {
		position, err := api.GetPosition(ctx, s.Request.OrderID)
		if err != nil {
			return err
		}
		if err = s.Request.Terms.CheckPosition(position, c.now()); err != nil {
			return c.review(ctx, s, err)
		}
	}
	s.Phase = PhaseTaking
	if err := c.Store.SaveRoboSatsSession(ctx, s); err != nil {
		return err
	}
	pos, err := api.TakeOfferWithAmount(ctx, s.Request.OrderID, s.Request.FiatAmount)
	if err != nil {
		existing, lookupErr := api.GetPosition(ctx, s.Request.OrderID)
		if lookupErr != nil || existing == nil || !existing.IsTaker || !existing.IsSeller {
			return fmt.Errorf("robosats: take was not confirmed: %w", err)
		}
		pos = existing
	}
	if pos.ID != s.Request.OrderID || !pos.IsTaker || !pos.IsSeller {
		return c.review(ctx, s, fmt.Errorf("coordinator returned a different order id"))
	}
	s.CoordinatorStatus = pos.Status
	s.RobotNick = pos.UrNick
	s.Phase = PhaseTaken
	return c.Store.SaveRoboSatsSession(ctx, s)
}

func (c *Coordinator) advancePayments(ctx context.Context, s *Session, api OrderAPI) error {
	backoff := 3 * time.Second
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pos, err := api.GetPosition(ctx, s.Request.OrderID)
		if err != nil {
			if IsPermanent(err) {
				return c.review(ctx, s, err)
			}
			if sleepErr := c.sleep(ctx, backoff); sleepErr != nil {
				return sleepErr
			}
			if backoff < 30*time.Second {
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
			continue
		}
		backoff = 3 * time.Second
		if pos.ID != s.Request.OrderID || !pos.IsTaker || !pos.IsSeller {
			return c.review(ctx, s, permanent("robosats: unexpected order or participant role"))
		}
		if pos.ExpiresAt != "" {
			deadline, err := time.Parse(time.RFC3339Nano, pos.ExpiresAt)
			if err != nil {
				return c.review(ctx, s, permanent("robosats: invalid coordinator deadline"))
			}
			s.Deadline = deadline
		}
		if !s.Deadline.IsZero() && !c.now().Before(s.Deadline) {
			return c.review(ctx, s, permanent("robosats: funding deadline expired"))
		}
		s.CoordinatorStatus = pos.Status
		if err := pos.ValidateResponse(); err != nil {
			return c.review(ctx, s, err)
		}
		if err := c.syncInvoices(ctx, s, pos); err != nil {
			return err
		}
		if err := c.submitPending(ctx, s); err != nil {
			return err
		}
		if pos.BondLocked() && s.Payments["bond"] != nil {
			s.Phase = PhaseBondLocked
		}
		if pos.EscrowLocked && s.Payments["escrow"] != nil && s.Payments["escrow"].Submitted {
			s.Funded = true
			s.Phase = PhaseEscrowLocked
			if pos.ChatReady() {
				s.Phase = PhaseChatReady
			}
			return c.Store.SaveRoboSatsSession(ctx, s)
		}
		if err := c.Store.SaveRoboSatsSession(ctx, s); err != nil {
			return err
		}
		if sleepErr := c.sleep(ctx, backoff); sleepErr != nil {
			return sleepErr
		}
	}
}

func (c *Coordinator) syncInvoices(ctx context.Context, s *Session, pos *Position) error {
	for purpose, invoice := range map[string]string{"bond": pos.BondInvoice, "escrow": pos.EscrowInvoice} {
		if previous := s.Payments[purpose]; previous != nil && invoice != "" && strings.TrimSpace(invoice) != previous.Invoice {
			return c.review(ctx, s, permanent("robosats: coordinator changed the %s invoice", purpose))
		}
	}
	if inv := strings.TrimSpace(pos.BondInvoice); inv != "" && s.Payments["bond"] == nil {
		pay, err := c.recordInvoice(ctx, s, "bond", inv, s.Request.MaxBondSat)
		if err != nil {
			return err
		}
		s.Payments["bond"] = pay
		s.Phase = PhaseTaken
	}
	if inv := strings.TrimSpace(pos.EscrowInvoice); inv != "" && s.Payments["escrow"] == nil {
		if s.Payments["bond"] == nil || !pos.BondLocked() {
			return nil
		}
		pay, err := c.recordInvoice(ctx, s, "escrow", inv, s.Request.MaxEscrowSat)
		if err != nil {
			if s.Payments["bond"] != nil {
				s.LastError = err.Error()
				s.Phase = PhaseNeedsReview
				_ = c.Store.SaveRoboSatsSession(ctx, s)
				return fmt.Errorf("robosats: escrow exceeds confirmed cap after bond lock; trade parked for review: %w", err)
			}
			return err
		}
		s.Payments["escrow"] = pay
	}
	return c.Store.SaveRoboSatsSession(ctx, s)
}

func (c *Coordinator) recordInvoice(ctx context.Context, s *Session, purpose, invoice string, cap int64) (*Payment, error) {
	decoded, err := c.Wallet.DecodeInvoice(ctx, invoice)
	if err != nil {
		return nil, fmt.Errorf("robosats: decode %s invoice: %w", purpose, err)
	}
	amountSat, amountMsat, err := validateHoldInvoice(invoice, decoded, cap, c.now())
	if err != nil {
		return nil, permanent("%s", err)
	}
	if s.Request.NodePubKey != "" && !strings.EqualFold(decoded.Destination, s.Request.NodePubKey) {
		return nil, permanent("robosats: invoice destination differs from pinned coordinator")
	}
	ts, _ := strconv.ParseInt(decoded.Timestamp, 10, 64)
	expiry, _ := strconv.ParseInt(decoded.Expiry, 10, 64)
	return &Payment{
		Purpose:    purpose,
		Invoice:    invoice,
		Hash:       strings.TrimSpace(decoded.PaymentHash),
		AmountSat:  amountSat,
		AmountMsat: amountMsat,
		Status:     "requested",
		ExpiresAt:  time.Unix(ts, 0).Add(time.Duration(expiry) * time.Second),
	}, nil
}

func (c *Coordinator) submitPending(ctx context.Context, s *Session) error {
	for _, purpose := range []string{"bond", "escrow"} {
		p := s.Payments[purpose]
		if p == nil {
			continue
		}
		if p.Status == "IN_FLIGHT" {
			status, err := c.Wallet.TrackHoldPayment(ctx, p.Hash)
			if err != nil {
				return err
			}
			p.Status = status
			if status != "IN_FLIGHT" && status != "SUCCEEDED" {
				return c.review(ctx, s, fmt.Errorf("%s hold payment changed to %s", purpose, status))
			}
			continue
		}
		if p.Status == "SUCCEEDED" || p.Submitted {
			continue
		}
		if p.Attempted {
			// A lost observer response may hide an accepted HTLC, including
			// after invoice expiry. Reconcile first, before checking expiry or
			// considering another send of this same payment hash.
			status, err := c.Wallet.TrackHoldPayment(ctx, p.Hash)
			if err != nil {
				return err
			}
			if status == "IN_FLIGHT" || status == "SUCCEEDED" {
				p.Status, p.Submitted = status, true
				if err := c.Store.SaveRoboSatsSession(ctx, s); err != nil {
					return err
				}
				continue
			}
			if status != "NOT_FOUND" {
				return c.review(ctx, s, permanent("robosats: %s payment state %s", purpose, status))
			}
		}
		p.Attempted = true
		if err := c.Store.SaveRoboSatsSession(ctx, s); err != nil {
			return err
		}
		fee := s.Request.BondFeeSat
		if purpose == "escrow" {
			fee = s.Request.EscrowFeeSat
		}
		// Legacy in-memory callers split the immutable total, never the invoice set.
		if s.Request.QuoteID == "" && s.Request.BondFeeSat == 0 && s.Request.EscrowFeeSat == 0 {
			fee = s.Request.RoutingFeeSat / 2
			if purpose == "escrow" {
				fee = s.Request.RoutingFeeSat - fee
			}
		}
		if !p.ExpiresAt.IsZero() && !p.ExpiresAt.After(c.now().Add(30*time.Second)) {
			return c.review(ctx, s, permanent("robosats: %s invoice expired", purpose))
		}
		status, err := c.Wallet.HoldPayment(ctx, p.Invoice, p.Hash, fee)
		if err != nil {
			return err
		}
		p.Status = status
		p.Submitted = true
		if purpose == "bond" {
			s.Phase = PhaseBondSubmitted
		} else {
			s.Phase = PhaseEscrowSubmitted
		}
		if err := c.Store.SaveRoboSatsSession(ctx, s); err != nil {
			return err
		}
		if status != "IN_FLIGHT" && status != "SUCCEEDED" {
			return c.review(ctx, s, fmt.Errorf("unexpected %s payment state %s", purpose, status))
		}
	}
	return nil
}

func (c *Coordinator) ReconcileChat(ctx context.Context, requestID uint64, template, ownNick string) (resultErr error) {
	if err := c.Ready(); err != nil {
		return err
	}
	unlock, err := c.Store.LockRoboSats(ctx, requestID)
	if err != nil {
		return err
	}
	defer unlock()
	s, err := c.Store.LoadRoboSatsSession(ctx, requestID)
	if err != nil || s == nil {
		if err == nil {
			err = fmt.Errorf("robosats: session not found")
		}
		return err
	}
	if !s.Funded {
		return fmt.Errorf("robosats: chat requires confirmed escrow lock")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		s.ChatError = ""
		if resultErr != nil {
			s.ChatError = resultErr.Error()
		}
		if err := c.Store.SaveRoboSatsSession(cleanup, s); resultErr == nil {
			resultErr = err
		}
	}()
	if s.ChatSent {
		return nil
	}
	if strings.TrimSpace(template) == "" {
		return fmt.Errorf("robosats: empty chat template")
	}
	if s.ChatTemplate == "" {
		s.ChatTemplate = template
	}
	template = s.ChatTemplate
	if err := c.Store.SaveRoboSatsSession(ctx, s); err != nil {
		return err
	}
	api, err := c.SessionAPI(ctx, s)
	if err != nil {
		return err
	}
	if _, err := api.GetRobot(ctx); err != nil {
		return err
	}
	position, err := api.GetPosition(ctx, s.Request.OrderID)
	if err != nil {
		return err
	}
	if position.ID != s.Request.OrderID || !position.IsTaker || !position.IsSeller || !position.ChatReady() {
		return fmt.Errorf("robosats: authenticated seller chat is not ready")
	}
	thread, err := api.GetChatMessages(ctx, s.Request.OrderID)
	if err != nil {
		return err
	}
	if ownMessageContains(thread, template, ownNick, s.RobotNick) {
		s.ChatSent = true
		s.Phase = PhaseChatSent
		return c.Store.SaveRoboSatsSession(ctx, s)
	}
	if err := api.SendChatMessage(ctx, s.Request.OrderID, template); err != nil {
		return err
	}
	s.ChatSent = true
	s.Phase = PhaseChatSent
	return c.Store.SaveRoboSatsSession(ctx, s)
}

func (c *Coordinator) review(ctx context.Context, s *Session, err error) error {
	s.Phase = PhaseNeedsReview
	s.LastError = err.Error()
	_ = c.Store.SaveRoboSatsSession(ctx, s)
	return err
}

func (c *Coordinator) resolveURL(id string) (string, error) {
	if c.Registry == nil {
		return "", fmt.Errorf("robosats: coordinator registry is not configured")
	}
	url, ok := c.Registry[strings.TrimSpace(id)]
	if !ok || strings.TrimSpace(url) == "" {
		return "", fmt.Errorf("robosats: coordinator %q is not in the federation registry", id)
	}
	return url, nil
}

func validateTakeRequest(r TakeRequest) error {
	if r.RequestID == 0 {
		return fmt.Errorf("robosats: durable transaction id required")
	}
	if strings.TrimSpace(r.CoordinatorID) == "" || strings.TrimSpace(r.OrderID) == "" {
		return fmt.Errorf("robosats: coordinator and order id are required")
	}
	if strings.TrimSpace(r.FiatAmount) == "" || r.FiatCurrency == "" || r.PaymentMethod == "" {
		return fmt.Errorf("robosats: selected fiat terms are required")
	}
	if r.MaxEscrowSat <= 0 || r.MaxBondSat < 0 || r.RoutingFeeSat < 0 || r.CredentialID <= 0 || r.BondFeeSat < 0 || r.EscrowFeeSat < 0 || r.BondFeeSat > r.RoutingFeeSat || r.EscrowFeeSat > r.RoutingFeeSat-r.BondFeeSat {
		return fmt.Errorf("robosats: payment ceilings and robot credential are required")
	}
	return nil
}

func sameTakeRequest(a, b TakeRequest) bool {
	if a.TokenFingerprint == "" {
		a.TokenFingerprint = b.TokenFingerprint
	}
	if b.TokenFingerprint == "" {
		b.TokenFingerprint = a.TokenFingerprint
	}
	return reflect.DeepEqual(a, b)
}

func tokenFingerprint(token string) string {
	digest := sha256.Sum256([]byte(AuthHeaderToken(token)))
	return hex.EncodeToString(digest[:])
}

type recoveryIdentity struct{ Token, PublicKey, PrivateKey string }

func (c *Coordinator) CheckBound(ctx context.Context, s *Session) error {
	if s.Phase != PhaseCreated {
		return nil
	}
	api, err := c.SessionAPI(ctx, s)
	if err != nil {
		return err
	}
	robot, err := api.GetRobot(ctx)
	if err != nil {
		return err
	}
	if robotString(robot, "active_order_id") != "" {
		return fmt.Errorf("robosats: robot is already active")
	}
	pos, err := api.GetPosition(ctx, s.Request.OrderID)
	if err != nil {
		return err
	}
	if s.Request.Terms == nil {
		return fmt.Errorf("robosats: immutable native terms unavailable")
	}
	return s.Request.Terms.CheckPosition(pos, c.now())
}

// SnapshotIdentity is called before admission commits and before any financial side effect.
func (c *Coordinator) SnapshotIdentity(ctx context.Context, s *Session) error {
	token, err := c.Token(ctx, s.Request.CredentialID)
	if err != nil {
		return err
	}
	api, err := c.Dial(s.Request.CoordinatorID, token)
	if err != nil {
		return err
	}
	if _, err = api.GetRobot(ctx); err != nil {
		return err
	}
	id := api.Identity()
	if id == nil || id.PublicKeyArmored == "" || id.EncPrivKeyArmored == "" {
		return fmt.Errorf("robosats: registered identity unavailable")
	}
	data, err := json.Marshal(recoveryIdentity{token, id.PublicKeyArmored, id.EncPrivKeyArmored})
	if err != nil {
		return err
	}
	s.RecoveryEncrypted, err = credentialcrypto.Encrypt(string(data))
	s.Request.TokenFingerprint = tokenFingerprint(token)
	if err != nil {
		return err
	}
	s.Request.CoordinatorURL, err = c.resolveURL(s.Request.CoordinatorID)
	return err
}

// SessionAPI never consults rotating credentials when durable recovery material exists.
func (c *Coordinator) SessionAPI(ctx context.Context, s *Session) (OrderAPI, error) {
	if s.RecoveryEncrypted == "" {
		if s.Request.QuoteID != "" {
			return nil, permanent("robosats: legacy session lacks recovery identity; manual review required")
		}
		token, err := c.Token(ctx, s.Request.CredentialID)
		if err != nil {
			return nil, err
		}
		if s.Request.TokenFingerprint != "" && tokenFingerprint(token) != s.Request.TokenFingerprint {
			return nil, permanent("robosats: robot credential changed")
		}
		return c.Dial(s.Request.CoordinatorID, token)
	}
	plain, err := credentialcrypto.Decrypt(s.RecoveryEncrypted)
	if err != nil {
		return nil, fmt.Errorf("robosats: recovery identity cannot be decrypted")
	}
	var saved recoveryIdentity
	if json.Unmarshal([]byte(plain), &saved) != nil {
		return nil, fmt.Errorf("robosats: invalid recovery identity")
	}
	var api OrderAPI
	if c.DialPinned != nil {
		api, err = c.DialPinned(s.Request.CoordinatorURL, saved.Token)
	} else {
		api, err = c.Dial(s.Request.CoordinatorID, saved.Token)
	}
	if err != nil {
		return nil, err
	}
	if client, ok := api.(*Client); ok {
		id, err := IdentityFromStoredKeys(saved.Token, saved.PublicKey, saved.PrivateKey)
		if err != nil {
			return nil, err
		}
		client.identity = id
		client.identityPinned = true
	}
	return api, nil
}

func firstNonEmpty(v string, fallback string) string {
	if strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

func ownMessageContains(thread *ChatThread, template string, nicks ...string) bool {
	if thread == nil || strings.TrimSpace(template) == "" {
		return false
	}
	nickSet := map[string]struct{}{}
	for _, n := range nicks {
		n = strings.TrimSpace(n)
		if n != "" {
			nickSet[strings.ToLower(n)] = struct{}{}
		}
	}
	for _, m := range thread.Messages {
		if !m.Own {
			continue
		}
		if len(nickSet) > 0 {
			if _, ok := nickSet[strings.ToLower(strings.TrimSpace(m.Nick))]; !ok {
				continue
			}
		}
		if m.Plaintext == template {
			return true
		}
	}
	return false
}

func (p *Position) IsParticipant() bool {
	return p != nil && (p.IsTaker || p.IsMaker)
}
