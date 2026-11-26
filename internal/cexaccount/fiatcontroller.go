package cexaccount

import (
	"commons/apierror"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// HTTPFiatControllerChecker checks liveness and all notification channels
// selected on the resolved payment account.
type HTTPFiatControllerChecker struct {
	baseURL string
	secret  string
	client  *http.Client
}

func NewHTTPFiatControllerChecker(baseURL, secret string, client *http.Client) *HTTPFiatControllerChecker {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPFiatControllerChecker{baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"), secret: secret, client: client}
}

func (c *HTTPFiatControllerChecker) Check(ctx context.Context, paymentMethod string) error {
	if err := c.CheckService(ctx); err != nil {
		return err
	}
	return c.CheckNotificationHealth(ctx, paymentMethod)
}

// CheckService verifies that the Fiat Controller HTTP service is reachable.
func (c *HTTPFiatControllerChecker) CheckService(ctx context.Context) error {
	if c.baseURL == "" {
		return fmt.Errorf("fiat Controller URL is not configured; set it in the Fiat Controller integration settings")
	}
	if err := c.get(ctx, "/health", nil); err != nil {
		return fmt.Errorf("the Fiat Controller service is unreachable: %w; verify the service is running and its URL is configured correctly", err)
	}
	return nil
}

// CheckNotificationHealth verifies all selected payment-notification channels.
func (c *HTTPFiatControllerChecker) CheckNotificationHealth(ctx context.Context, paymentMethod string) error {
	var health struct {
		SelectedChannels []string `json:"selected_channels"`
		Channels         map[string]struct {
			Healthy bool `json:"healthy"`
		} `json:"channels"`
	}
	path := "/notification-health?payment_method=" + url.QueryEscape(strings.TrimSpace(paymentMethod))
	if err := c.get(ctx, path, &health); err != nil {
		return fmt.Errorf("could not read payment notification health from Fiat Controller: %w", err)
	}
	if len(health.SelectedChannels) == 0 {
		return fmt.Errorf(
			"the payment account for %s has no notification channel selected; select at least one channel in the payment method categorization panel",
			paymentMethod,
		)
	}
	for _, channel := range health.SelectedChannels {
		state, ok := health.Channels[channel]
		if !ok {
			return fmt.Errorf(
				"notification channel %q has no health status yet; send a test notification so Fiat Controller records a delivery",
				channel,
			)
		}
		if !state.Healthy {
			return fmt.Errorf(
				"notification channel %q is unhealthy: its most recent delivery attempt failed or no delivery has ever succeeded; check the channel configuration in Fiat Controller and send a test notification",
				channel,
			)
		}
	}
	return nil
}

func (c *HTTPFiatControllerChecker) get(ctx context.Context, path string, output any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	if c.secret != "" {
		req.Header.Set("X-Webhook-Secret", c.secret)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return apierror.FromResponse(resp, "the Fiat Controller")
	}
	if output != nil {
		if err := json.NewDecoder(resp.Body).Decode(output); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
