package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type rateLimitWindow struct {
	ResetAt     *int64 `json:"reset_at,omitempty"`
	UsedPercent int    `json:"used_percent"`
}

type rateLimitStatus struct {
	Primary   *rateLimitWindow `json:"primary,omitempty"`
	Secondary *rateLimitWindow `json:"secondary,omitempty"`
	LimitID   string           `json:"limit_id,omitempty"`
}

type rateLimitReading struct {
	ResetsAt    *int64
	UsedPercent int
}

// A rate limit snapshot is sparse. Null windows do not
// invalidate recent readings, but windows from different meters cannot mix.
type rateLimitSnapshot struct {
	Primary, Secondary *rateLimitReading
	LimitID            string
}

func (s *rateLimitStatus) update(snapshot rateLimitSnapshot) {
	if snapshot.LimitID != "" && snapshot.LimitID != s.LimitID {
		if s.LimitID != "" {
			*s = rateLimitStatus{}
		}
		s.LimitID = snapshot.LimitID
	}
	if snapshot.Primary != nil {
		s.Primary = &rateLimitWindow{UsedPercent: snapshot.Primary.UsedPercent, ResetAt: snapshot.Primary.ResetsAt}
	}
	if snapshot.Secondary != nil {
		s.Secondary = &rateLimitWindow{UsedPercent: snapshot.Secondary.UsedPercent, ResetAt: snapshot.Secondary.ResetsAt}
	}
}

type rateLimitProjection struct {
	ResetAt     *int64 `json:"reset_at,omitempty"`
	RateLimited bool   `json:"rate_limited"`
}

type rateLimitView struct {
	rateLimitProjection
	rateLimitStatus
}

func (s *rateLimitStatus) windowResets(now int64) (next, blockedUntil *int64) {
	for _, window := range [2]*rateLimitWindow{s.Primary, s.Secondary} {
		if window == nil || window.ResetAt == nil || *window.ResetAt <= now {
			continue
		}
		if next == nil || *window.ResetAt < *next {
			next = window.ResetAt
		}
		if window.UsedPercent >= 100 && (blockedUntil == nil || *window.ResetAt > *blockedUntil) {
			blockedUntil = window.ResetAt
		}
	}
	return next, blockedUntil
}

func (s status) rateLimit(now int64) rateLimitProjection {
	next, blockedUntil := s.windowResets(now)
	view := rateLimitProjection{ResetAt: next, RateLimited: s.CompletedRateLimited || blockedUntil != nil}
	if blockedUntil != nil {
		view.ResetAt = blockedUntil
	}
	return view
}

func (s status) rateLimitView(now int64) rateLimitView {
	return rateLimitView{rateLimitProjection: s.rateLimit(now), rateLimitStatus: s.rateLimitStatus}
}

func (e turnError) rateLimited() bool {
	if e.RateLimited {
		return true
	}
	var kind string
	if json.Unmarshal(e.ErrorInfo, &kind) == nil && kind != "" {
		return kind == "usageLimitExceeded" || kind == "rateLimitExceeded"
	}
	var variants map[string]struct{ HTTPStatusCode int }
	if json.Unmarshal(e.ErrorInfo, &variants) == nil {
		for _, v := range variants {
			if v.HTTPStatusCode == 429 {
				return true
			}
		}
	}
	// Older app-server versions may send a message without an error type.
	if len(e.ErrorInfo) == 0 || string(e.ErrorInfo) == "null" {
		msg := strings.ToLower(e.Message)
		return strings.Contains(msg, "rate limit exceeded") || strings.Contains(msg, "usage limit exceeded")
	}
	return false
}

func (r *runner) noteRateLimit(turnID, source string, retry bool, failure *turnError) error {
	if failure == nil || !failure.rateLimited() {
		return nil
	}
	return r.localEvent("turn/rate_limited", fmt.Sprintf("turn=%s source=%s retry=%t %s", turnID, source, retry, brief(r.c.secret.text(failure.Message))))
}
