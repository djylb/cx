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
	ResetAt     *int64           `json:"reset_at,omitempty"`
	Primary     *rateLimitWindow `json:"primary,omitempty"`
	Secondary   *rateLimitWindow `json:"secondary,omitempty"`
	LimitID     string           `json:"limit_id,omitempty"`
	RateLimited bool             `json:"rate_limited"`
}

type rateLimitReading struct {
	ResetsAt    *int64
	UsedPercent int
}

// A rate limit snapshot is sparse. Null windows do not
// invalidate recent readings, but windows from different meters cannot mix.
type rateLimitSnapshot struct {
	Primary, Secondary   *rateLimitReading
	SpendControlReached  *bool
	LimitID              string
	RateLimitReachedType string
}

func (s *rateLimitStatus) update(snapshot rateLimitSnapshot, now int64) {
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
	if snapshot.Primary == nil && snapshot.Secondary == nil && snapshot.RateLimitReachedType == "" && snapshot.SpendControlReached == nil {
		return
	}
	var next, blockedUntil *int64
	var exhausted, unknownReset bool
	for _, window := range []*rateLimitWindow{s.Primary, s.Secondary} {
		if window == nil || (window.ResetAt != nil && *window.ResetAt <= now) {
			continue
		}
		if window.ResetAt != nil && (next == nil || *window.ResetAt < *next) {
			next = window.ResetAt
		}
		if window.UsedPercent >= 100 {
			exhausted = true
			if window.ResetAt == nil {
				unknownReset = true
			} else if blockedUntil == nil || *window.ResetAt > *blockedUntil {
				blockedUntil = window.ResetAt
			}
		}
	}
	s.RateLimited = exhausted || snapshot.RateLimitReachedType != "" || (snapshot.SpendControlReached != nil && *snapshot.SpendControlReached)
	s.ResetAt = next
	if exhausted {
		s.ResetAt = blockedUntil
		if unknownReset {
			s.ResetAt = nil
		}
	}
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
	r.s.RateLimited = true
	return r.localEvent("turn/rate_limited", fmt.Sprintf("turn=%s source=%s retry=%t %s", turnID, source, retry, brief(r.c.secret.text(failure.Message))))
}
