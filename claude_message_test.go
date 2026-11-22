package main

import (
	"encoding/json"
	"testing"
)

func TestClaudeEventStringMessage(t *testing.T) {
	for _, in := range []string{
		`{"type":"system","message":"compacting conversation"}`,
		`{"type":"assistant","message":null}`,
	} {
		var ev claudeEvent
		if err := json.Unmarshal([]byte(in), &ev); err != nil || len(ev.Message.Content) != 0 {
			t.Fatalf("%s: %v %+v", in, err, ev.Message)
		}
	}
	var ev claudeEvent
	if err := json.Unmarshal([]byte(`{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}`), &ev); err != nil || len(ev.Message.Content) != 1 || ev.Message.Content[0].Text != "hi" {
		t.Fatalf("%v %+v", err, ev.Message)
	}
}
