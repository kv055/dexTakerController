package robosats

import (
	"commons/apierror"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const DefaultFederationURL = "https://raw.githubusercontent.com/RoboSats/robosats/main/frontend/static/federation.json"

func LoadFederation(ctx context.Context, client *http.Client, registry map[string]string, source string) (map[string]string, error) {
	u, err := url.Parse(source)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("invalid RoboSats federation URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("RoboSats federation registry: %w", apierror.FromResponse(res, "the RoboSats federation registry"))
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	return MergeFederation(registry, body)
}

// RegistryFromOnions keys coordinators by onion hostname so a configured
// ROBOSATS_URL still resolves, and federation aliases can be merged on top.
func RegistryFromOnions(onions []string) map[string]string {
	out := make(map[string]string, len(onions))
	for _, onion := range onions {
		id, ok := coordinatorIDFromURL(onion)
		if !ok {
			continue
		}
		out[id] = strings.TrimRight(onion, "/")
	}
	return out
}

func coordinatorIDFromURL(raw string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return "", false
	}
	return strings.TrimSuffix(u.Host, ".onion"), true
}

// MergeFederation inserts stable federation aliases (object keys) mapped to
// their mainnet onion URLs. Alias keys win for execution; onion-host keys remain
// as a fallback.
func MergeFederation(registry map[string]string, body []byte) (map[string]string, error) {
	if registry == nil {
		registry = map[string]string{}
	}
	var entries map[string]struct {
		Federated bool `json:"federated"`
		Mainnet   struct {
			Onion string `json:"onion"`
		} `json:"mainnet"`
	}
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("robosats: decode federation registry: %w", err)
	}
	for id, entry := range entries {
		if !entry.Federated {
			continue
		}
		onion := strings.TrimRight(strings.TrimSpace(entry.Mainnet.Onion), "/")
		u, err := url.Parse(onion)
		if err != nil || u.Scheme != "http" || !strings.HasSuffix(u.Hostname(), ".onion") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			continue
		}
		registry[strings.TrimSpace(id)] = onion
		if host, ok := coordinatorIDFromURL(onion); ok {
			registry[host] = onion
		}
	}
	return registry, nil
}

func TradeRef(coordinatorID, orderID string) string {
	coordinatorID = strings.TrimSpace(coordinatorID)
	orderID = strings.TrimSpace(orderID)
	if coordinatorID == "" {
		return orderID
	}
	return coordinatorID + ":" + orderID
}

func SplitTradeRef(ref string) (coordinatorID, orderID string) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", ""
	}
	coordinatorID, orderID, ok := strings.Cut(ref, ":")
	if !ok {
		return "", ref
	}
	return strings.TrimSpace(coordinatorID), strings.TrimSpace(orderID)
}

func ParseCoordinatorPairs(spec string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, url, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(id)] = strings.TrimRight(strings.TrimSpace(url), "/")
	}
	return out
}
