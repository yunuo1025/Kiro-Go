package proxy

import (
	"encoding/json"
	"kiro-go/config"
	accountpool "kiro-go/pool"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// setupIntegrityPathTest installs a single-account config plus a fake upstream
// and returns a handler wired to the reloaded pool.
func setupIntegrityPathTest(t *testing.T, server *httptest.Server) *Handler {
	t.Helper()
	if err := config.Init(t.TempDir() + "/config.json"); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.AddAccount(config.Account{
		ID:          "test-account",
		Enabled:     true,
		AccessToken: "token-test",
		ProfileArn:  "arn:aws:codewhisperer:profile/test",
	}); err != nil {
		t.Fatalf("add account: %v", err)
	}
	if err := config.UpdatePreferredEndpoint("kiro"); err != nil {
		t.Fatalf("set preferred endpoint: %v", err)
	}
	if err := config.UpdateEndpointFallback(false); err != nil {
		t.Fatalf("disable endpoint fallback: %v", err)
	}
	t.Cleanup(swapKiroEndpointsForTest(t, server))

	p := accountpool.GetPool()
	p.Reload()
	return &Handler{
		pool:        p,
		promptCache: newPromptCacheTracker(defaultPromptCacheTTL),
	}
}

// reasoningOnlyUpstream ends cleanly after reasoning without an answer. Unlike
// content-only EOF, this remains an integrity failure. Thinking-enabled Claude
// and OpenAI streams flush the reasoning, making retries unsafe.
func reasoningOnlyUpstream(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(awsEventStreamFrame(t, "reasoningContentEvent", map[string]interface{}{
			"text": strings.Repeat("partial reasoning ", 8),
		}))
	}))
}

// A truncated stream whose content already reached the client must end with an
// SSE error, never with a forged end_turn that tells the client it is done.
func TestClaudeStreamEmitsErrorOnTruncatedStream(t *testing.T) {
	var hits atomic.Int32
	server := reasoningOnlyUpstream(t, &hits)
	defer server.Close()
	h := setupIntegrityPathTest(t, server)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{
		"model":"claude-sonnet-4.5-thinking",
		"max_tokens":100,
		"messages":[{"role":"user","content":"hello"}],
		"stream":true
	}`))
	rec := httptest.NewRecorder()
	h.handleClaudeMessages(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "partial reasoning") {
		t.Fatalf("expected flushed content, got %s", body)
	}
	if strings.Contains(body, `"stop_reason":"end_turn"`) {
		t.Fatalf("truncated stream must not be reported as end_turn, got %s", body)
	}
	if !strings.Contains(body, `"type":"error"`) {
		t.Fatalf("expected SSE error event, got %s", body)
	}
	// Content was already flushed, so reissuing would duplicate output.
	if hits.Load() != 1 {
		t.Fatalf("must not retry after client flush, hits=%d", hits.Load())
	}
}

// Non-stream buffers everything, so a truncated first attempt is safe to retry
// on the same account. The client must receive the recovered answer only.
func TestClaudeNonStreamRetriesTruncatedStream(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		w.WriteHeader(http.StatusOK)
		if n == 1 {
			_, _ = w.Write(awsEventStreamFrame(t, "reasoningContentEvent", map[string]interface{}{
				"text": "truncated attempt",
			}))
			return
		}
		_, _ = w.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{
			"content": "recovered answer",
		}))
		_, _ = w.Write(awsEventStreamFrame(t, "metadataEvent", map[string]interface{}{
			"stopReason": "end_turn",
		}))
	}))
	defer server.Close()
	h := setupIntegrityPathTest(t, server)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{
		"model":"claude-sonnet-4.5-thinking",
		"max_tokens":100,
		"messages":[{"role":"user","content":"hello"}]
	}`))
	rec := httptest.NewRecorder()
	h.handleClaudeMessages(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after recovery, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp ClaudeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected content, got %+v", resp)
	}
	text := resp.Content[0].Text
	if strings.Contains(text, "truncated attempt") {
		t.Fatalf("first attempt must be discarded on retry, got %q", text)
	}
	if !strings.Contains(text, "recovered answer") {
		t.Fatalf("expected recovered answer, got %q", text)
	}
	if hits.Load() < 2 {
		t.Fatalf("expected same-account retry, hits=%d", hits.Load())
	}
}

// A soft integrity failure means the upstream blipped, not that the credential
// is bad. The account must stay enabled and unbanned.
func TestIntegrityFailureDoesNotBanAccount(t *testing.T) {
	server := reasoningOnlyUpstream(t, nil)
	defer server.Close()
	h := setupIntegrityPathTest(t, server)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{
		"model":"claude-sonnet-4.5-thinking",
		"max_tokens":100,
		"messages":[{"role":"user","content":"hello"}],
		"stream":true
	}`))
	h.handleClaudeMessages(httptest.NewRecorder(), req)

	var account *config.Account
	for _, acc := range config.GetAccounts() {
		if acc.ID == "test-account" {
			found := acc
			account = &found
			break
		}
	}
	if account == nil {
		t.Fatal("account disappeared")
	}
	if !account.Enabled {
		t.Fatal("integrity failure must not disable the account")
	}
	if account.BanStatus == "BANNED" || account.BanStatus == "DISABLED" {
		t.Fatalf("integrity failure must not ban the account, status=%q reason=%q",
			account.BanStatus, account.BanReason)
	}
}

// Responses streaming must surface response.failed instead of closing the turn
// with response.completed when the upstream truncated.
func TestResponsesStreamEmitsFailedOnTruncatedStream(t *testing.T) {
	server := reasoningOnlyUpstream(t, nil)
	defer server.Close()
	h := setupIntegrityPathTest(t, server)

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"claude-sonnet-4.5-thinking",
		"input":"hello",
		"stream":true,
		"store":false
	}`))
	rec := httptest.NewRecorder()
	h.handleOpenAIResponses(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "response.failed") {
		t.Fatalf("expected response.failed, got %s", body)
	}
	if strings.Contains(body, "response.completed") {
		t.Fatalf("truncated stream must not report response.completed, got %s", body)
	}
}

// OpenAI streaming must not close a truncated turn with a normal finish_reason.
func TestOpenAIStreamEmitsErrorOnTruncatedStream(t *testing.T) {
	server := reasoningOnlyUpstream(t, nil)
	defer server.Close()
	h := setupIntegrityPathTest(t, server)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{
		"model":"claude-sonnet-4.5-thinking",
		"messages":[{"role":"user","content":"hello"}],
		"stream":true
	}`))
	rec := httptest.NewRecorder()
	h.handleOpenAIChat(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "partial reasoning") {
		t.Fatalf("expected flushed content, got %s", body)
	}
	if strings.Contains(body, `"finish_reason":"stop"`) {
		t.Fatalf("truncated stream must not finish with stop, got %s", body)
	}
	if strings.Contains(body, "[DONE]") {
		t.Fatalf("truncated stream must not report completion, got %s", body)
	}
	if !strings.Contains(body, `"error"`) {
		t.Fatalf("expected an SSE error, got %s", body)
	}
}

// A buffered reasoning-only attempt is retryable. Its reasoning must be reset
// before recovery, otherwise thinking-enabled responses expose the discarded
// attempt even though the final answer succeeds.
func TestClaudeNonStreamRetryDoesNotLeakPreviousAttemptReasoning(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		w.WriteHeader(http.StatusOK)
		if n == 1 {
			_, _ = w.Write(awsEventStreamFrame(t, "reasoningContentEvent", map[string]interface{}{
				"text": "LEAK",
			}))
			return
		}
		_, _ = w.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{
			"content": "clean answer",
		}))
		_, _ = w.Write(awsEventStreamFrame(t, "metadataEvent", map[string]interface{}{
			"stopReason": "end_turn",
		}))
	}))
	defer server.Close()
	h := setupIntegrityPathTest(t, server)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{
		"model":"claude-sonnet-4.5-thinking",
		"max_tokens":100,
		"messages":[{"role":"user","content":"hello"}]
	}`))
	rec := httptest.NewRecorder()
	h.handleClaudeMessages(rec, req)

	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("expected successful recovery, got %d body=%s", rec.Code, body)
	}
	var resp ClaudeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode recovered answer: %v body=%s", err, body)
	}
	if resp.StopReason != "end_turn" {
		t.Fatalf("expected completed answer, got %+v", resp)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected one retry for reasoning-only stream, hits=%d", hits.Load())
	}
	if strings.Contains(body, "LEAK") {
		t.Fatalf("discarded attempt leaked into the retry output: %s", body)
	}
	if !strings.Contains(body, "clean answer") {
		t.Fatalf("expected recovered answer, got %s", body)
	}
}

// Issue #158: Kiro can finish an answer with usage events and clean EOF, without
// metadataEvent. Neither usage events nor an explicit stop reason are required
// for a content-bearing response to complete successfully on any public path.
func TestContentCleanEOFCompletesAcrossAPIPaths(t *testing.T) {
	const answer = "complete answer"
	for _, api := range []struct {
		name string
		path string
	}{
		{"claude", "/v1/messages"},
		{"openai", "/v1/chat/completions"},
		{"responses", "/v1/responses"},
	} {
		for _, streaming := range []bool{false, true} {
			for _, usageEvents := range []bool{false, true} {
				name := api.name + "/nonstream/content_only"
				if streaming {
					name = api.name + "/stream/content_only"
				}
				if usageEvents {
					name = strings.TrimSuffix(name, "content_only") + "content_context_metering"
				}
				t.Run(name, func(t *testing.T) {
					var hits atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						hits.Add(1)
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{
							"content": answer,
						}))
						if usageEvents {
							_, _ = w.Write(awsEventStreamFrame(t, "contextUsageEvent", map[string]interface{}{
								"contextUsagePercentage": 12.5,
							}))
							_, _ = w.Write(awsEventStreamFrame(t, "meteringEvent", map[string]interface{}{
								"usage": 1.25,
							}))
						}
						// No metadataEvent: returning ends the body at a frame boundary.
					}))
					defer server.Close()
					h := setupIntegrityPathTest(t, server)
					payload := map[string]interface{}{
						"model": "claude-sonnet-4.5", "stream": streaming,
						"max_tokens": 100,
						"messages":   []map[string]string{{"role": "user", "content": "hello"}},
					}
					if api.name == "responses" {
						delete(payload, "messages")
						delete(payload, "max_tokens")
						payload["input"] = "hello"
						payload["store"] = false
					}
					requestBody, err := json.Marshal(payload)
					if err != nil {
						t.Fatal(err)
					}
					req := httptest.NewRequest(http.MethodPost, api.path, strings.NewReader(string(requestBody)))
					rec := httptest.NewRecorder()
					switch api.name {
					case "claude":
						h.handleClaudeMessages(rec, req)
					case "openai":
						h.handleOpenAIChat(rec, req)
					case "responses":
						h.handleOpenAIResponses(rec, req)
					}
					body := rec.Body.String()
					if hits.Load() != 1 {
						t.Fatalf("complete content must not retry, upstream hits=%d body=%s", hits.Load(), body)
					}
					if rec.Code != http.StatusOK || strings.Contains(body, `"error"`) || strings.Contains(body, "response.failed") {
						t.Fatalf("expected successful answer, status=%d body=%s", rec.Code, body)
					}
					if !strings.Contains(body, answer) {
						t.Fatalf("missing answer: %s", body)
					}
					if usageEvents && h.getCredits() != 1.25 {
						t.Fatalf("expected metering to survive clean EOF, credits=%v", h.getCredits())
					}
					if streaming {
						if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/event-stream") {
							t.Fatalf("expected SSE, got %q", rec.Header().Get("Content-Type"))
						}
						for _, line := range strings.Split(body, "\n") {
							if strings.HasPrefix(line, "data: ") && line != "data: [DONE]" && !json.Valid([]byte(strings.TrimPrefix(line, "data: "))) {
								t.Fatalf("invalid SSE JSON: %s", line)
							}
						}
						var markers []string
						switch api.name {
						case "claude":
							markers = []string{`"stop_reason":"end_turn"`, "event: message_stop"}
						case "openai":
							markers = []string{`"finish_reason":"stop"`, "data: [DONE]"}
						case "responses":
							markers = []string{"event: response.completed", `"status":"completed"`, "data: [DONE]"}
						}
						for _, marker := range markers {
							if !strings.Contains(body, marker) {
								t.Fatalf("missing completion marker %q: %s", marker, body)
							}
						}
						return
					}
					switch api.name {
					case "claude":
						var resp ClaudeResponse
						if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
							t.Fatal(err)
						}
						if resp.StopReason != "end_turn" || len(resp.Content) != 1 || resp.Content[0].Text != answer {
							t.Fatalf("unexpected completed answer: %+v", resp)
						}
					case "openai":
						var resp OpenAIResponse
						if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
							t.Fatal(err)
						}
						if len(resp.Choices) != 1 || resp.Choices[0].FinishReason != "stop" || resp.Choices[0].Message.Content != answer {
							t.Fatalf("unexpected completed answer: %+v", resp)
						}
					case "responses":
						var resp ResponsesObject
						if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
							t.Fatal(err)
						}
						if resp.Status != "completed" || len(resp.Output) != 1 || len(resp.Output[0].Content) != 1 || resp.Output[0].Content[0].Text != answer {
							t.Fatalf("unexpected completed answer: %+v", resp)
						}
					}
				})
			}
		}
	}
}
