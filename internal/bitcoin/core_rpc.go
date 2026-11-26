package bitcoin

import (
	"bytes"
	"commons/apierror"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// CoreRPCClient checks the same Bitcoin Core RPC endpoint configured for LND.
type CoreRPCClient struct {
	endpoint string
	username string
	password string
	client   *http.Client
}

func NewCoreRPCClient(endpoint, username, password string) *CoreRPCClient {
	return &CoreRPCClient{
		endpoint: strings.TrimSpace(endpoint),
		username: username,
		password: password,
		client:   &http.Client{Timeout: 15 * time.Second},
	}
}

// Ping verifies that the configured Core RPC endpoint accepts authenticated
// requests. Bitcoin Core's ping RPC also asks its P2P peers to measure latency.
func (c *CoreRPCClient) Ping(ctx context.Context) error {
	if c == nil || c.endpoint == "" {
		return fmt.Errorf("bitcoin Core RPC endpoint is not configured")
	}
	if strings.TrimSpace(c.username) == "" || strings.TrimSpace(c.password) == "" {
		return fmt.Errorf("bitcoin Core RPC credentials are not configured")
	}

	body, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      string `json:"id"`
		Method  string `json:"method"`
	}{JSONRPC: "1.0", ID: "lnd-health", Method: "ping"})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create Bitcoin Core ping request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("bitcoin Core ping request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bitcoin Core ping: %w", apierror.FromResponse(resp, "Bitcoin Core"))
	}
	var payload struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return fmt.Errorf("decode Bitcoin Core ping response: %w", err)
	}
	if payload.Error != nil {
		return fmt.Errorf("bitcoin Core ping: %s", payload.Error.Message)
	}
	return nil
}
