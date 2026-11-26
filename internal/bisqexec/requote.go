package bisqexec

import (
	"bytes"
	"commons/apierror"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"commons/bisqroute"
	"dexTakerController/internal/db"
)

type RouteQuoter interface {
	RequoteBisq(context.Context, bisqroute.Request) (*db.ResolvedRoute, error)
}

type HTTPRouteQuoter struct {
	BaseURL, Secret string
	Client          *http.Client
}

func (q HTTPRouteQuoter) RequoteBisq(ctx context.Context, in bisqroute.Request) (*db.ResolvedRoute, error) {
	if strings.TrimSpace(q.BaseURL) == "" || q.Secret == "" {
		return nil, fmt.Errorf("authenticated Unified API route quoter is not configured")
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(q.BaseURL, "/")+"/internal/bisq/requote", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Internal-Secret", q.Secret)
	req.Header.Set("Content-Type", "application/json")
	client := q.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		upstream := apierror.FromResponse(resp, "the Unified API")
		reason := upstream.Reason
		if reason == "" {
			reason = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		return nil, apierror.Wrapf(upstream, "bisq_requote_failed",
			"The Unified API couldn't re-price this Bisq offer, so the trade was not started: %s. Refresh the offer and request a new Bisq quote.", strings.TrimSuffix(reason, ".")).
			WithStatus(http.StatusConflict)
	}
	var route db.ResolvedRoute
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&route); err != nil {
		return nil, err
	}
	if err := route.ValidateAuthoritativeSnapshot(); err != nil {
		return nil, err
	}
	return &route, nil
}

func (s *Service) WithRouteQuoter(q RouteQuoter) *Service { s.routeQuoter = q; return s }
