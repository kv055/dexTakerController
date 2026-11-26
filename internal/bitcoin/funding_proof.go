package bitcoin

import (
	"bytes"
	"commons/apierror"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

var ErrFundingUnconfirmed = errors.New("funding transaction is not sufficiently confirmed")

type FundingProof struct {
	TxID          string `json:"txid"`
	Address       string `json:"address"`
	AmountSats    uint64 `json:"amount_sats"`
	Confirmations int64  `json:"confirmations"`
}

// ProveFunding checks the transaction hash and actual output scripts locally.
// A withdrawal status, wallet balance, or txid alone is not proof of payment.
// Confirmation information still trusts the configured chain data source.
func (ndf *NetworkDataFetcher) ProveFunding(ctx context.Context, txid, address string, minimumSats uint64, confirmations int64) (*FundingProof, error) {
	if len(txid) != 64 || minimumSats == 0 || minimumSats > 21_000_000*100_000_000 || confirmations < 1 {
		return nil, errors.New("invalid funding proof request")
	}
	if _, err := hex.DecodeString(txid); err != nil {
		return nil, err
	}
	a, err := btcutil.DecodeAddress(address, &chaincfg.MainNetParams)
	if err != nil || !a.IsForNet(&chaincfg.MainNetParams) {
		return nil, errors.New("invalid mainnet funding address")
	}
	script, err := txscript.PayToAddrScript(a)
	if err != nil {
		return nil, err
	}
	get := func(path string) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ndf.apiBase()+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", ndf.userAgent)
		resp, err := ndf.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrFundingUnconfirmed
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("chain proof: %w", apierror.FromResponse(resp, "the block explorer"))
		}
		return io.ReadAll(io.LimitReader(resp.Body, 8_000_001))
	}
	raw, err := get("/api/tx/" + txid + "/hex")
	if err != nil {
		return nil, err
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(decoded) > 4_000_000 {
		return nil, errors.New("invalid funding transaction encoding")
	}
	var tx wire.MsgTx
	reader := bytes.NewReader(decoded)
	if err := tx.Deserialize(reader); err != nil {
		return nil, err
	}
	if reader.Len() != 0 || tx.TxHash().String() != txid {
		return nil, errors.New("funding transaction hash mismatch")
	}
	var paid, total uint64
	for _, out := range tx.TxOut {
		if out.Value < 0 || out.Value > 21_000_000*100_000_000 {
			return nil, errors.New("invalid transaction output amount")
		}
		total += uint64(out.Value)
		if total > 21_000_000*100_000_000 {
			return nil, errors.New("transaction output sum out of range")
		}
		if bytes.Equal(out.PkScript, script) {
			paid += uint64(out.Value)
		}
	}
	if paid < minimumSats {
		return nil, fmt.Errorf("funding output pays %d sats; need %d", paid, minimumSats)
	}
	raw, err = get("/api/tx/" + txid + "/status")
	if err != nil {
		return nil, err
	}
	var status struct {
		Confirmed bool  `json:"confirmed"`
		Height    int64 `json:"block_height"`
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return nil, err
	}
	if !status.Confirmed || status.Height <= 0 {
		return nil, ErrFundingUnconfirmed
	}
	tip, err := ndf.fetchTipHeight(ctx)
	if err != nil {
		return nil, err
	}
	if tip < status.Height || tip-status.Height+1 < confirmations {
		return nil, ErrFundingUnconfirmed
	}
	return &FundingProof{TxID: txid, Address: address, AmountSats: paid, Confirmations: tip - status.Height + 1}, nil
}
