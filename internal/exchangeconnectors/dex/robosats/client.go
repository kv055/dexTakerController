package robosats

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultTimeout = 60 * time.Second

// DefaultCoordinatorURLs lists the known RoboSats mainnet coordinators. Robot
// tokens are coordinator-specific, so callers without a configured base URL
// can probe these in order. All coordinators are Tor onion services and
// require a SOCKS proxy (Config.ProxyURL).
var DefaultCoordinatorURLs = []string{
	"http://ngdk7ocdzmz5kzsysa3om6du7ycj2evxp2f2olfkyq37htx3gllwp2yd.onion",
	"http://mmhaqzuirth5rx7gl24d4773lknltjhik57k7ahec5iefktezv4b3uid.onion",
	"http://4t4jxmivv6uqej6xzx2jx3fxh75gtt65v3szjoqmc4ugdlhipzdat6yd.onion",
	"http://otmoonrndnrddqdlhu6b36heunmbyw3cgvadqo2oqeau3656wfv7fwad.onion",
}

// Config represents a config.
type Config struct {
	// BaseURL is the base url value for the config.
	BaseURL string
	// AuthToken is the raw robot token. The client derives the
	// base91-encoded SHA256 hash RoboSats expects in the Authorization header.
	AuthToken string
	// ProxyURL is an optional SOCKS proxy (e.g. socks5h://127.0.0.1:9050),
	// required to reach the Tor onion coordinators.
	ProxyURL string
	// TakeOfferPath is unused; taking uses OrderActionPath. Kept for config compatibility.
	TakeOfferPath string
	// OpenPositionsPath is the open positions path value for the config.
	OpenPositionsPath string
	// PositionPath is unused; polling uses OrderActionPath. Kept for config compatibility.
	PositionPath string
	// CreateOrderPath is the create order path value for the config.
	CreateOrderPath string
	// OrderActionPath is the singular order endpoint. Take, poll, and confirm all
	// use POST/GET /api/order/?order_id=<id>.
	OrderActionPath string
	// RobotPath is the robot path value for the config.
	RobotPath string
	// ChatPath is the PGP trade-chat endpoint, format string taking the order id. GET reads
	// the (encrypted) messages + peer public key; POST sends an encrypted message.
	ChatPath string
}

// Client represents a client.
type Client struct {
	baseURL           string
	authToken         string
	takeOfferPath     string
	openPositionsPath string
	positionPath      string
	createOrderPath   string
	orderActionPath   string
	robotPath         string
	chatPath          string
	http              *http.Client
	proxyErr          error

	identityMu     sync.Mutex
	identity       *Identity
	identityPinned bool
}

// Robot identifies a robot. Coordinators return public_key and
// encrypted_private_key for an already-registered identity.
type Robot map[string]any

// Position is a RoboSats order as returned by GET/POST /api/order/.
type Position struct {
	ID             string          `json:"id"`
	OfferID        string          `json:"offer_id"`
	Type           int             `json:"type"`
	Status         int             `json:"status"`
	Amount         json.Number     `json:"amount"`
	MinAmount      json.Number     `json:"min_amount"`
	MaxAmount      json.Number     `json:"max_amount"`
	Price          json.Number     `json:"price"`
	Premium        json.Number     `json:"premium"`
	PaymentMethod  string          `json:"payment_method"`
	BondInvoice    string          `json:"bond_invoice,omitempty"`
	EscrowInvoice  string          `json:"escrow_invoice,omitempty"`
	BondSatoshis   int64           `json:"bond_satoshis,omitempty"`
	EscrowSatoshis int64           `json:"escrow_satoshis,omitempty"`
	TradeSatoshis  int64           `json:"trade_satoshis,omitempty"`
	MakerLocked    bool            `json:"maker_locked"`
	TakerLocked    bool            `json:"taker_locked"`
	EscrowLocked   bool            `json:"escrow_locked"`
	IsSeller       bool            `json:"is_seller"`
	IsBuyer        bool            `json:"is_buyer"`
	IsTaker        bool            `json:"is_taker"`
	IsMaker        bool            `json:"is_maker"`
	HasRange       bool            `json:"has_range"`
	ExpiresAt      string          `json:"expires_at,omitempty"`
	StatusMessage  string          `json:"status_message,omitempty"`
	UrNick         string          `json:"ur_nick,omitempty"`
	BadRequest     string          `json:"bad_request,omitempty"`
	ChatLastIndex  int             `json:"chat_last_index,omitempty"`
	Raw            json.RawMessage `json:"-"`
}

// UnmarshalJSON accepts numeric or string id/type/status and retains the
// complete coordinator payload, including top-level bond/escrow invoices.
func (p *Position) UnmarshalJSON(data []byte) error {
	var wire struct {
		ID             flexibleValue `json:"id"`
		OfferID        flexibleValue `json:"offer_id"`
		Type           flexibleValue `json:"type"`
		Status         flexibleValue `json:"status"`
		Amount         json.Number   `json:"amount"`
		MinAmount      json.Number   `json:"min_amount"`
		MaxAmount      json.Number   `json:"max_amount"`
		Price          json.Number   `json:"price"`
		Premium        json.Number   `json:"premium"`
		PaymentMethod  string        `json:"payment_method"`
		BondInvoice    string        `json:"bond_invoice"`
		EscrowInvoice  string        `json:"escrow_invoice"`
		BondSatoshis   int64         `json:"bond_satoshis"`
		EscrowSatoshis int64         `json:"escrow_satoshis"`
		TradeSatoshis  int64         `json:"trade_satoshis"`
		MakerLocked    bool          `json:"maker_locked"`
		TakerLocked    bool          `json:"taker_locked"`
		EscrowLocked   bool          `json:"escrow_locked"`
		IsSeller       bool          `json:"is_seller"`
		IsBuyer        bool          `json:"is_buyer"`
		IsTaker        bool          `json:"is_taker"`
		IsMaker        bool          `json:"is_maker"`
		HasRange       bool          `json:"has_range"`
		ExpiresAt      string        `json:"expires_at"`
		StatusMessage  string        `json:"status_message"`
		UrNick         string        `json:"ur_nick"`
		BadRequest     string        `json:"bad_request"`
		ChatLastIndex  int           `json:"chat_last_index"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	p.ID = wire.ID.String()
	p.OfferID = wire.OfferID.String()
	var err error
	p.Type, err = strconv.Atoi(wire.Type.String())
	if err != nil && wire.BadRequest == "" {
		return permanent("robosats: missing or invalid order type")
	}
	p.Status, err = strconv.Atoi(wire.Status.String())
	if err != nil && wire.BadRequest == "" {
		return permanent("robosats: missing or invalid order status")
	}
	p.Amount = wire.Amount
	p.MinAmount = wire.MinAmount
	p.MaxAmount = wire.MaxAmount
	p.Price = wire.Price
	p.Premium = wire.Premium
	p.PaymentMethod = wire.PaymentMethod
	p.BondInvoice = strings.TrimSpace(wire.BondInvoice)
	p.EscrowInvoice = strings.TrimSpace(wire.EscrowInvoice)
	p.BondSatoshis = wire.BondSatoshis
	p.EscrowSatoshis = wire.EscrowSatoshis
	p.TradeSatoshis = wire.TradeSatoshis
	p.MakerLocked = wire.MakerLocked
	p.TakerLocked = wire.TakerLocked
	p.EscrowLocked = wire.EscrowLocked
	p.IsSeller = wire.IsSeller
	p.IsBuyer = wire.IsBuyer
	p.IsTaker = wire.IsTaker
	p.IsMaker = wire.IsMaker
	p.HasRange = wire.HasRange
	p.ExpiresAt = wire.ExpiresAt
	p.StatusMessage = wire.StatusMessage
	p.UrNick = wire.UrNick
	p.BadRequest = wire.BadRequest
	p.ChatLastIndex = wire.ChatLastIndex
	p.Raw = append(json.RawMessage(nil), data...)
	return nil
}

type flexibleValue []byte

func (v *flexibleValue) UnmarshalJSON(data []byte) error {
	*v = append((*v)[:0], data...)
	return nil
}

func (v flexibleValue) String() string {
	if len(v) == 0 || string(v) == "null" {
		return ""
	}
	var s string
	if json.Unmarshal(v, &s) == nil {
		return s
	}
	var n json.Number
	if json.Unmarshal(v, &n) == nil {
		return n.String()
	}
	return strings.TrimSpace(string(v))
}

func (v flexibleValue) Int() int {
	s := v.String()
	if s == "" {
		return 0
	}
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

// CreatePositionParams represents a create position params.
type CreatePositionParams struct {
	// Type is the type value for the create position params.
	Type int `json:"type"`
	// Currency names the asset or network for the create position params.
	Currency int `json:"currency"`
	// Amount is the amount value for the create position params.
	Amount int64 `json:"amount,omitempty"`
	// MinAmount is the min amount value for the create position params.
	MinAmount int64 `json:"min_amount,omitempty"`
	// MaxAmount is the max amount value for the create position params.
	MaxAmount int64 `json:"max_amount,omitempty"`
	// Price is the price value used by the create position params.
	Price string `json:"price,omitempty"`
	// Premium is the premium value for the create position params.
	Premium string `json:"premium,omitempty"`
	// PaymentMethod is the payment method value for the create position params.
	PaymentMethod string `json:"payment_method,omitempty"`
}

// NewClient creates a client.
func NewClient(cfg Config) *Client {
	client := &Client{
		baseURL:           strings.TrimRight(cfg.BaseURL, "/"),
		authToken:         cfg.AuthToken,
		takeOfferPath:     defaultPath(cfg.TakeOfferPath, "/api/order/?order_id=%s"),
		openPositionsPath: defaultPath(cfg.OpenPositionsPath, "/api/orders/"),
		positionPath:      defaultPath(cfg.PositionPath, "/api/order/?order_id=%s"),
		createOrderPath:   defaultPath(cfg.CreateOrderPath, "/api/orders/"),
		orderActionPath:   defaultPath(cfg.OrderActionPath, "/api/order/?order_id=%s"),
		robotPath:         defaultPath(cfg.RobotPath, "/api/robot/"),
		chatPath:          defaultPath(cfg.ChatPath, "/api/chat/?order_id=%s"),
		http:              &http.Client{Timeout: defaultTimeout},
	}
	if proxy := strings.TrimSpace(cfg.ProxyURL); proxy != "" {
		// net/http understands the socks5 scheme and resolves hostnames at
		// the proxy, which onion addresses require; socks5h is an alias.
		proxyURL, err := url.Parse(strings.Replace(proxy, "socks5h://", "socks5://", 1))
		if err != nil {
			client.proxyErr = fmt.Errorf("robosats: invalid proxy URL: %w", err)
		} else {
			client.http.Transport = &http.Transport{Proxy: http.ProxyURL(proxyURL)}
		}
	}
	return client
}

// AuthHeaderToken returns the credential RoboSats expects in the
// Authorization header: the SHA256 hash of the raw robot token, base91-encoded.
func AuthHeaderToken(token string) string {
	digest := sha256.Sum256([]byte(token))
	return base91Encode(digest[:])
}

// WithAuthToken configures the RoboSats authentication token.
func (c *Client) WithAuthToken(authToken string) *Client {
	return &Client{
		baseURL:           c.baseURL,
		authToken:         authToken,
		takeOfferPath:     c.takeOfferPath,
		openPositionsPath: c.openPositionsPath,
		positionPath:      c.positionPath,
		createOrderPath:   c.createOrderPath,
		orderActionPath:   c.orderActionPath,
		robotPath:         c.robotPath,
		chatPath:          c.chatPath,
		http:              c.http,
		proxyErr:          c.proxyErr,
	}
}

// robotIdentity lazily derives (and caches) the robot identity for the
// configured token. Coordinators require the full identity to create the
// robot on first contact.
func (c *Client) robotIdentity() (*Identity, error) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	if c.identity != nil {
		return c.identity, nil
	}
	identity, err := NewIdentity(c.authToken)
	if err != nil {
		return nil, err
	}
	c.identity = identity
	return identity, nil
}

// Identity returns the current robot identity, generating one only if the
// coordinator has not yet restored registered keys.
func (c *Client) Identity() *Identity {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.identity
}

func (c *Client) restoreIdentityFromRobot(robot Robot) error {
	if robot == nil || c.authToken == "" {
		return nil
	}
	publicKey, _ := robot["public_key"].(string)
	encPrivKey, _ := robot["encrypted_private_key"].(string)
	if strings.TrimSpace(publicKey) == "" || strings.TrimSpace(encPrivKey) == "" {
		return nil
	}
	identity, err := IdentityFromStoredKeys(c.authToken, publicKey, encPrivKey)
	if err != nil {
		return err
	}
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	if c.identityPinned && (c.identity == nil || c.identity.PublicKeyArmored != identity.PublicKeyArmored || c.identity.EncPrivKeyArmored != identity.EncPrivKeyArmored) {
		return permanent("robosats: registered identity differs from admitted robot")
	}
	c.identity = identity
	return nil
}

func robotString(robot Robot, keys ...string) string {
	for _, key := range keys {
		v := robot[key]
		if v == nil {
			continue
		}
		s := strings.TrimSpace(fmt.Sprint(v))
		if s != "" && s != "0" {
			return s
		}
	}
	return ""
}

// GetRobot retrieves a robot and restores the coordinator-registered PGP
// identity when the response includes stored keys.
func (c *Client) GetRobot(ctx context.Context) (Robot, error) {
	var robot Robot
	if err := c.request(ctx, http.MethodGet, c.robotPath, nil, &robot); err != nil {
		return nil, err
	}
	if err := c.restoreIdentityFromRobot(robot); err != nil {
		return nil, err
	}
	return robot, nil
}

// TakeOffer accepts an existing DEX offer using action "take". Ranged offers
// require amount as the selected fiat quantity.
func (c *Client) TakeOffer(ctx context.Context, offerID string) (*Position, error) {
	return c.TakeOfferWithAmount(ctx, offerID, "")
}

// TakeOfferWithAmount takes the order and, when amount is non-empty, sends the
// selected fiat amount required for ranged offers.
func (c *Client) TakeOfferWithAmount(ctx context.Context, offerID, amount string) (*Position, error) {
	if offerID == "" {
		return nil, errors.New("robosats: offer ID is required")
	}
	body := map[string]any{"action": "take"}
	if strings.TrimSpace(amount) != "" {
		body["amount"] = strings.TrimSpace(amount)
	}
	var pos Position
	if err := c.request(ctx, http.MethodPost, c.orderPath(offerID), body, &pos); err != nil {
		return nil, err
	}
	if err := pos.ValidateResponse(); err != nil {
		return nil, err
	}
	return &pos, nil
}

// GetOpenPositions retrieves an open positions.
func (c *Client) GetOpenPositions(ctx context.Context) ([]Position, error) {
	var positions []Position
	if err := c.request(ctx, http.MethodGet, c.openPositionsPath, nil, &positions); err != nil {
		return nil, err
	}
	return positions, nil
}

// GetPosition retrieves a position.
func (c *Client) GetPosition(ctx context.Context, positionID string) (*Position, error) {
	if positionID == "" {
		return nil, errors.New("robosats: position ID is required")
	}

	var pos Position
	if err := c.request(ctx, http.MethodGet, c.orderPath(positionID), nil, &pos); err != nil {
		return nil, err
	}
	if err := pos.ValidateResponse(); err != nil {
		return nil, err
	}
	return &pos, nil
}

// CreatePosition creates a position.
func (c *Client) CreatePosition(ctx context.Context, params CreatePositionParams) (*Position, error) {
	var pos Position
	if err := c.request(ctx, http.MethodPost, c.createOrderPath, params, &pos); err != nil {
		return nil, err
	}
	return &pos, nil
}

// ConfirmFiatReceived confirms — as the BTC seller — that the buyer's fiat payment has
// arrived, which tells the coordinator to settle the locked hold invoice to the buyer
// (releasing the sats). This is the RoboSats Stage-6 release for the swap route: the
// coordinator holds the keys, so no local signing is required. Returns the updated order.
func (c *Client) ConfirmFiatReceived(ctx context.Context, orderID string) (*Position, error) {
	if orderID == "" {
		return nil, errors.New("robosats: order ID is required")
	}
	var pos Position
	body := map[string]any{"action": "confirm"}
	if err := c.request(ctx, http.MethodPost, c.orderPath(orderID), body, &pos); err != nil {
		return nil, err
	}
	if err := pos.ValidateResponse(); err != nil {
		return nil, err
	}
	return &pos, nil
}

func (c *Client) orderPath(orderID string) string {
	path := c.orderActionPath
	if path == "" {
		path = c.positionPath
	}
	if path == "" {
		path = c.takeOfferPath
	}
	// The order ID comes from coordinator responses and is interpolated into
	// a URL template ("/api/order/?order_id=%s" by default). Escape it so a
	// crafted ID cannot smuggle path/query delimiters into the request.
	return fmt.Sprintf(path, url.QueryEscape(orderID))
}

func (c *Client) request(ctx context.Context, method, path string, body any, out any) error {
	if c.baseURL == "" {
		return errors.New("robosats: base URL is required")
	}
	if c.proxyErr != nil {
		return c.proxyErr
	}

	var payload bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&payload).Encode(body); err != nil {
			return fmt.Errorf("robosats: encode request body: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, &payload)
	if err != nil {
		return fmt.Errorf("robosats: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.authToken != "" {
		// Send the full identity, like the RoboSats web client: the
		// coordinator creates the robot on first contact and ignores the
		// keys once it exists.
		identity, err := c.robotIdentity()
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", identity.AuthHeader())
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("robosats: %s %s failed: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 408 && resp.StatusCode != 429 {
			// Keep the coordinator's explanation: on 4xx it says why it refused
			// (order taken or cancelled, robot not allowed, invalid amount, ...).
			detail := readErrorDetail(resp.Body)
			msg := fmt.Sprintf("robosats: %s %s returned HTTP %d", method, path, resp.StatusCode)
			if detail != "" {
				msg += " (" + detail + ")"
			}
			return &ProtocolError{Message: msg, Status: resp.StatusCode, Detail: detail}
		}
		if detail := readErrorDetail(resp.Body); detail != "" {
			return fmt.Errorf("robosats: %s %s returned HTTP %d (%s)", method, path, resp.StatusCode, detail)
		}
		return fmt.Errorf("robosats: %s %s returned HTTP %d", method, path, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out); err != nil {
		return permanent("robosats: invalid response: %v", err)
	}
	return nil
}

// readErrorDetail extracts the human-readable error message RoboSats returns
// in error bodies ({"bad_request": ...} or {"detail": ...}), or "" if absent.
func readErrorDetail(body io.Reader) string {
	var envelope struct {
		BadRequest string `json:"bad_request"`
		Detail     string `json:"detail"`
	}
	if err := json.NewDecoder(io.LimitReader(body, 4096)).Decode(&envelope); err != nil {
		return ""
	}
	if envelope.BadRequest != "" {
		return envelope.BadRequest
	}
	return envelope.Detail
}

func defaultPath(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}
