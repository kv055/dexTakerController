package kraken

import (
	"commons/apierror"
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"dexTakerController/internal/exchangeconnectors/cex"
	"github.com/krakenfx/api-go/v2/pkg/derivatives"
	krakenapi "github.com/krakenfx/api-go/v2/pkg/kraken"
)

const defaultFuturesBaseURL = "https://futures.kraken.com"

const (
	FuturesAccessNone     = "NO_ACCESS"
	FuturesAccessReadOnly = "READ_ONLY"
	FuturesAccessFull     = "FULL_ACCESS"
)

// FuturesClient adapts Kraken's official derivatives SDK. Its REST and WebSocket
// implementations own request signing and transport.
type FuturesClient struct {
	rest      *derivatives.REST
	apiKey    string
	apiSecret string
	nonce     *krakenapi.EpochCounter
	http      *http.Client
}

type FuturesAPIKeyInfo struct {
	APIKey      string                   `json:"apiKey"`
	AccountUID  string                   `json:"accountUid"`
	Permissions FuturesAPIKeyPermissions `json:"permissions"`
}

type FuturesAPIKeyPermissions struct {
	General  string `json:"general"`
	Transfer string `json:"transfer"`
}

func NewFuturesClient(apiKey, apiSecret string) *FuturesClient {
	return NewFuturesClientWithBaseURL("", apiKey, apiSecret)
}

func NewFuturesClientWithBaseURL(baseURL, apiKey, apiSecret string) *FuturesClient {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = os.Getenv("KRAKEN_FUTURES_API_BASE_URL")
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultFuturesBaseURL
	}
	rest := derivatives.NewREST()
	rest.BaseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	rest.PublicKey = strings.TrimSpace(apiKey)
	rest.PrivateKey = strings.TrimSpace(apiSecret)
	httpClient := &http.Client{Timeout: 15 * time.Second}
	return &FuturesClient{rest: rest, apiKey: rest.PublicKey, apiSecret: rest.PrivateKey, nonce: krakenapi.NewEpochCounter(), http: httpClient}
}

type contextualREST struct {
	rest   *derivatives.REST
	ctx    context.Context
	client *http.Client
	nonce  func() string
}

func (r contextualREST) NewRequest(opts derivatives.RequestOptions) (*krakenapi.Request, error) {
	clone := *r.rest
	clone.Executor = func(req *http.Request) (*http.Response, error) {
		return r.client.Do(req.WithContext(r.ctx))
	}
	opts.Nonce = r.nonce
	return clone.NewRequest(opts)
}

func callKraken[T any](c *FuturesClient, ctx context.Context, opts derivatives.RequestOptions) (*derivatives.Response[T], error) {
	resp, err := derivatives.Call[T](contextualREST{rest: c.rest, ctx: ctx, client: c.http, nonce: c.nonce.Get}, opts)
	// Check the status before the decode error: a rejected order carries
	// Kraken's reason (e.g. insufficientAvailableFunds) or an HTML error page.
	if resp != nil && resp.Http != nil && resp.Http.Response != nil {
		if status := resp.Http.StatusCode; status < 200 || status >= 300 {
			return nil, fmt.Errorf("kraken futures: %w", apierror.FromBody(status, resp.Http.Body, "Kraken Futures"))
		}
	}
	if err != nil {
		return nil, err
	}
	if resp.Http == nil || resp.Http.Response == nil {
		return nil, fmt.Errorf("kraken futures: missing HTTP response")
	}
	return resp, nil
}

// CheckAPIKey uses the SDK's generic authenticated REST call because v2.0.0 has
// no dedicated wrapper for Futures' separate auth service endpoint.
func (c *FuturesClient) CheckAPIKey(ctx context.Context) (*FuturesAPIKeyInfo, error) {
	resp, err := callKraken[FuturesAPIKeyInfo](c, ctx, derivatives.RequestOptions{
		Method: "GET", Path: "/api/auth/v1/api-keys/v3/check", Auth: true,
	})
	if err != nil {
		return nil, fmt.Errorf("kraken futures check api key: %w", err)
	}
	info := resp.Result
	if strings.TrimSpace(info.Permissions.General) == "" && strings.TrimSpace(info.Permissions.Transfer) == "" {
		return nil, fmt.Errorf("kraken futures check api key: empty permissions in response")
	}
	return &info, nil
}

func (c *FuturesClient) PlaceOrder(ctx context.Context, symbol, side string, btcNotional float64) (*cex.FuturesOrderResult, error) {
	markPrice, err := c.fetchMarkPrice(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("kraken futures mark price: %w", err)
	}
	contracts := int64(math.Round(btcNotional * markPrice))
	if contracts <= 0 {
		return nil, fmt.Errorf("kraken futures: computed contract size is zero (btcNotional=%.8f markPrice=%.2f)", btcNotional, markPrice)
	}
	krakenSide := "sell"
	if side == "long" {
		krakenSide = "buy"
	}
	type sendStatus struct {
		OrderID string `json:"order_id"`
		Status  string `json:"status"`
	}
	type sendResult struct {
		Result     string     `json:"result"`
		Error      string     `json:"error"`
		SendStatus sendStatus `json:"sendStatus"`
	}
	resp, err := callKraken[sendResult](c, ctx, derivatives.RequestOptions{
		Method: "POST", Path: "/derivatives/api/v3/sendorder", Auth: true,
		Body: &derivatives.OrderRequest{OrderType: "mkt", Symbol: symbol, Side: krakenSide, Size: strconv.FormatInt(contracts, 10)},
	})
	if err != nil {
		return nil, fmt.Errorf("kraken futures sendorder: %w", err)
	}
	if resp.Result.Result != "success" {
		return nil, fmt.Errorf("kraken futures sendorder: result=%q error=%q", resp.Result.Result, resp.Result.Error)
	}
	return &cex.FuturesOrderResult{Venue: "kraken_futures", Instrument: symbol, Side: side, OrderID: resp.Result.SendStatus.OrderID, Status: resp.Result.SendStatus.Status}, nil
}

func (c *FuturesClient) fetchMarkPrice(ctx context.Context, symbol string) (float64, error) {
	type ticker struct {
		Tag       string  `json:"tag"`
		MarkPrice float64 `json:"markPrice"`
	}
	type result struct {
		Tickers []ticker `json:"tickers"`
	}
	resp, err := callKraken[result](c, ctx, derivatives.RequestOptions{Method: "GET", Path: []any{"/derivatives/api/v3/tickers/", symbol}})
	if err != nil {
		return 0, err
	}
	if len(resp.Result.Tickers) == 0 || resp.Result.Tickers[0].MarkPrice <= 0 {
		return 0, fmt.Errorf("mark price is zero or negative")
	}
	return resp.Result.Tickers[0].MarkPrice, nil
}

// CurrentPosition uses the official SDK's authenticated request construction for
// the endpoint that is not modelled by v2.0.0 yet.
func (c *FuturesClient) CurrentPosition(ctx context.Context) (cex.HedgePositionUpdate, error) {
	type position struct {
		Side, Symbol string
		Price, Size  float64
	}
	type result struct {
		OpenPositions []position `json:"openPositions"`
		derivatives.DerivativesResponse
	}
	resp, err := callKraken[result](c, ctx, derivatives.RequestOptions{Method: "GET", Path: "/derivatives/api/v3/openpositions", Auth: true})
	if err != nil {
		return cex.HedgePositionUpdate{}, fmt.Errorf("kraken futures openpositions: %w", err)
	}
	if !strings.EqualFold(resp.Result.Result, "success") {
		return cex.HedgePositionUpdate{}, fmt.Errorf("kraken futures openpositions: result=%q", resp.Result.Result)
	}
	update := cex.HedgePositionUpdate{Venue: "kraken_futures", Instrument: hedgeInstrument, Side: "flat", Currency: "USD", UpdatedAt: time.Now().UTC().Format(time.RFC3339), Connected: true}
	for _, pos := range resp.Result.OpenPositions {
		if !strings.EqualFold(pos.Symbol, hedgeInstrument) {
			continue
		}
		update.Side, update.EntryPrice = strings.ToLower(pos.Side), pos.Price
		if pos.Price > 0 {
			update.SizeBTC = absFloat(pos.Size / pos.Price)
		}
		return update, nil
	}
	return update, nil
}
