package robosats

import (
	"commons/apierror"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Codes for operator-facing RoboSats failures. The dashboard branches on these.
const (
	CodeDisabled               = "robosats_disabled"
	CodeNoRobot                = "robosats_no_robot"
	CodeRobotRejected          = "robosats_robot_rejected"
	CodeRobotBusy              = "robosats_robot_busy"
	CodeOfferUnavailable       = "robosats_offer_unavailable"
	CodeOfferNotTakeable       = "robosats_offer_not_takeable"
	CodeOfferPassword          = "robosats_offer_password"
	CodeCurrencyChanged        = "robosats_offer_currency_changed"
	CodePaymentMethod          = "robosats_payment_method_mismatch"
	CodeInvalidTerms           = "robosats_invalid_terms"
	CodeAmountOutOfRange       = "robosats_amount_out_of_range"
	CodeInvalidAmount          = "invalid_amount"
	CodeOfferExpiring          = "robosats_offer_expiring"
	CodeCoordinatorUnverified  = "robosats_coordinator_unverified"
	CodeCoordinatorUnreachable = "robosats_coordinator_unreachable"
	CodeCoordinatorUnknown     = "robosats_coordinator_unknown"
	CodeCoordinatorRefused     = "robosats_coordinator_refused"
	CodeTermsChanged           = "robosats_terms_changed"
)

// ErrDisabled explains that the operator switched RoboSats execution off.
func ErrDisabled() error {
	return apierror.New(CodeDisabled, "RoboSats trading is turned off on this server. Set ROBOSATS_ENABLED=true for the Taker Controller and restart it to take RoboSats offers.").
		WithStatus(http.StatusServiceUnavailable)
}

// ErrNoRobot explains that no robot credential is configured.
func ErrNoRobot() error {
	return apierror.New(CodeNoRobot, "No RoboSats robot is set up. Add a RoboSats account with its robot token under Trading Accounts, then try again.").
		WithStatus(http.StatusConflict)
}

// ErrCoordinatorUnknown explains a coordinator missing from the federation registry.
func ErrCoordinatorUnknown(coordinatorID string, cause error) error {
	return apierror.Wrapf(cause, CodeCoordinatorUnknown,
		"The RoboSats coordinator %q is not in this server's federation list, so its offers can't be taken. Refresh the offer list; if it keeps happening, check ROBOSATS_FEDERATION_URL and ROBOSATS_COORDINATORS.", coordinatorID).
		WithStatus(http.StatusConflict)
}

func robotBusy(activeOrderID string) error {
	return apierror.Newf(CodeRobotBusy,
		"Your RoboSats robot already has an active order (#%s). Finish or cancel that order in RoboSats before taking another offer.", activeOrderID).
		WithStatus(http.StatusConflict)
}

// offerStateProblem explains why a fetched order cannot be taken as a maker-buy offer, or returns nil.
func offerStateProblem(p *Position, orderID string) error {
	switch {
	case p == nil || p.ID != orderID:
		return apierror.Newf(CodeOfferUnavailable, "RoboSats did not return offer #%s. Refresh the offer list and pick the offer again.", orderID).WithStatus(http.StatusConflict)
	case p.IsMaker:
		return apierror.Newf(CodeOfferNotTakeable, "Offer #%s was created by your own robot; you can't take your own offer.", orderID).WithStatus(http.StatusConflict)
	case p.IsTaker:
		return apierror.Newf(CodeOfferNotTakeable, "Your robot is already the taker of offer #%s. Continue that trade instead of starting a new one.", orderID).WithStatus(http.StatusConflict)
	case p.Type != TypeBuy:
		return apierror.Newf(CodeOfferNotTakeable, "Offer #%s sells bitcoin. This bot only takes RoboSats offers whose maker buys bitcoin.", orderID).WithStatus(http.StatusConflict)
	case p.Status != StatusPublic:
		return apierror.Newf(CodeOfferUnavailable, "Offer #%s is no longer open: %s. Refresh the offer list and pick another offer.", orderID, describeOrderStatus(p.Status)).WithStatus(http.StatusConflict)
	}
	return nil
}

func describeOrderStatus(status int) string {
	switch status {
	case StatusWaitingMakerBond:
		return "its maker has not locked their bond yet"
	case StatusPaused:
		return "its maker paused it"
	case StatusWaitingTakerBond:
		return "another robot is taking it right now"
	case StatusCancelled:
		return "it was cancelled"
	case StatusExpired:
		return "it expired"
	default:
		return "another robot already took it"
	}
}

// explainCoordinatorError turns a failed coordinator call into an operator
// message. action completes "Could not ...", e.g. "load your robot".
func explainCoordinatorError(err error, action string) error {
	if err == nil {
		return nil
	}
	if _, ok := apierror.As(err); ok {
		return err
	}
	status, detail := RejectedStatus(err)
	switch {
	case (status == http.StatusUnauthorized || status == http.StatusForbidden) && strings.Contains(action, "robot"):
		return apierror.Wrapf(err, CodeRobotRejected,
			"The RoboSats coordinator rejected your robot token (HTTP %d). Check the robot token of your RoboSats account under Trading Accounts.", status).
			WithStatus(http.StatusConflict)
	case status == http.StatusNotFound && strings.Contains(action, "offer"):
		return apierror.Wrap(err, CodeOfferUnavailable,
			"This RoboSats offer no longer exists on its coordinator (it was taken, cancelled or expired). Refresh the offer list and pick another offer.").
			WithStatus(http.StatusConflict)
	case status != 0:
		reason := detail
		if reason == "" {
			reason = fmt.Sprintf("HTTP %d", status)
		}
		return apierror.Wrapf(err, CodeCoordinatorRefused, "The RoboSats coordinator refused to %s: %s.", action, strings.TrimSuffix(reason, ".")).
			WithStatus(http.StatusConflict)
	case errors.Is(err, context.DeadlineExceeded) || apierror.IsUnreachable(err) || strings.Contains(strings.ToLower(err.Error()), "timeout"):
		return apierror.Wrapf(err, CodeCoordinatorUnreachable,
			"Could not reach the RoboSats coordinator over Tor to %s. The coordinator may be offline or Tor is still connecting; try again in a minute.", action).
			WithStatus(http.StatusBadGateway)
	case strings.Contains(err.Error(), "returned HTTP 5"):
		return apierror.Wrapf(err, CodeCoordinatorUnreachable,
			"The RoboSats coordinator had an internal error while trying to %s. Try again in a moment.", action).
			WithStatus(http.StatusBadGateway)
	}
	return apierror.Wrapf(err, CodeCoordinatorRefused, "Could not %s on the RoboSats coordinator.", action).WithStatus(http.StatusBadGateway)
}
