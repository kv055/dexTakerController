package bitcoin

import (
	"commons/apierror"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// NetworkDataFetcher handles Bitcoin network data API calls via mempool.space.
type NetworkDataFetcher struct {
	client    *http.Client
	userAgent string
	baseURL   string
}

// NewNetworkDataFetcher creates a new fetcher with mempool.space as the default source.
func NewNetworkDataFetcher() *NetworkDataFetcher {
	return &NetworkDataFetcher{
		client:    &http.Client{Timeout: 10 * time.Second},
		userAgent: "dexTakerController/1.0",
	}
}

func (ndf *NetworkDataFetcher) apiBase() string {
	if strings.TrimSpace(ndf.baseURL) != "" {
		return strings.TrimRight(ndf.baseURL, "/")
	}
	return "https://mempool.space"
}

// FetchAddressBalance returns the confirmed BTC balance of a Bitcoin address
// by querying the mempool.space public API. The returned value is in BTC (not satoshis).
func (ndf *NetworkDataFetcher) FetchAddressBalance(ctx context.Context, address string) (float64, error) {
	url := ndf.apiBase() + "/api/address/" + address

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("bitcoin: failed to create address request: %w", err)
	}
	req.Header.Set("User-Agent", ndf.userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := ndf.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("bitcoin: address balance request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("bitcoin: balance of address %s: %w", address, apierror.FromResponse(resp, "mempool.space"))
	}

	var ar struct {
		ChainStats struct {
			FundedSum int64 `json:"funded_txo_sum"`
			SpentSum  int64 `json:"spent_txo_sum"`
		} `json:"chain_stats"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return 0, fmt.Errorf("bitcoin: failed to decode address response: %w", err)
	}

	confirmedSatoshis := ar.ChainStats.FundedSum - ar.ChainStats.SpentSum
	return float64(confirmedSatoshis) / 1e8, nil
}

// TxStatus is a normalized on-chain transaction status from mempool.space.
type TxStatus struct {
	TxID          string `json:"txid"`
	Confirmed     bool   `json:"confirmed"`
	Confirmations int    `json:"confirmations"`
	BlockHeight   int64  `json:"block_height,omitempty"`
	StatusLabel   string `json:"status_label"`
}

// FetchTransaction returns mempool/confirmation status for a Bitcoin txid.
func (ndf *NetworkDataFetcher) FetchTransaction(ctx context.Context, txid string) (*TxStatus, error) {
	txid = strings.TrimSpace(txid)
	if txid == "" {
		return nil, fmt.Errorf("bitcoin: txid is required")
	}

	url := ndf.apiBase() + "/api/tx/" + txid
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("bitcoin: failed to create tx request: %w", err)
	}
	req.Header.Set("User-Agent", ndf.userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := ndf.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bitcoin: tx request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return &TxStatus{
			TxID:        txid,
			StatusLabel: "not_found",
		}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bitcoin: transaction %s: %w", txid, apierror.FromResponse(resp, "mempool.space"))
	}

	var payload struct {
		TxID   string `json:"txid"`
		Status struct {
			Confirmed   bool  `json:"confirmed"`
			BlockHeight int64 `json:"block_height"`
		} `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("bitcoin: failed to decode tx response: %w", err)
	}

	status := &TxStatus{
		TxID:        payload.TxID,
		Confirmed:   payload.Status.Confirmed,
		BlockHeight: payload.Status.BlockHeight,
	}
	if status.TxID == "" {
		status.TxID = txid
	}

	if !status.Confirmed {
		status.StatusLabel = "mempool"
		status.Confirmations = 0
		return status, nil
	}

	tip, err := ndf.fetchTipHeight(ctx)
	if err != nil {
		status.StatusLabel = "confirmed"
		status.Confirmations = 1
		return status, nil
	}
	if tip >= status.BlockHeight {
		status.Confirmations = int(tip-status.BlockHeight) + 1
	} else {
		status.Confirmations = 1
	}
	status.StatusLabel = "confirmed"
	return status, nil
}

// TransactionConfirmations adapts FetchTransaction to tradeflow's narrow
// confirmation source interface.
func (ndf *NetworkDataFetcher) TransactionConfirmations(ctx context.Context, txid string) (int, error) {
	status, err := ndf.FetchTransaction(ctx, txid)
	if err != nil || status == nil {
		return 0, err
	}
	return status.Confirmations, nil
}

func (ndf *NetworkDataFetcher) fetchTipHeight(ctx context.Context) (int64, error) {
	url := ndf.apiBase() + "/api/blocks/tip/height"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", ndf.userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := ndf.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("bitcoin: tip height: %w", apierror.FromResponse(resp, "mempool.space"))
	}
	var height int64
	if err := json.NewDecoder(resp.Body).Decode(&height); err != nil {
		return 0, err
	}
	return height, nil
}
