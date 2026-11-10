package proxy

import (
	"kiro-go/config"
	"net/http"
	"strings"
	"testing"
)

func TestBuildStreamingHeaderValuesAlignsWithKiroIDEFormat(t *testing.T) {
	account := &config.Account{MachineId: "machine-123"}
	values := buildStreamingHeaderValues(account, "q.us-east-1.amazonaws.com")

	if values.Host != "q.us-east-1.amazonaws.com" {
		t.Fatalf("expected host to be preserved, got %q", values.Host)
	}
	if !strings.Contains(values.UserAgent, "aws-sdk-js/1.0.34") {
		t.Fatalf("expected streaming sdk version in user agent, got %q", values.UserAgent)
	}
	if !strings.Contains(values.UserAgent, "api/codewhispererstreaming#1.0.34") {
		t.Fatalf("expected streaming API marker in user agent, got %q", values.UserAgent)
	}
	if !strings.Contains(values.UserAgent, "KiroIDE-1.4.121-machine-123") {
		t.Fatalf("expected kiro version and machine id in user agent, got %q", values.UserAgent)
	}
	if !strings.Contains(values.AmzUserAgent, "aws-sdk-js/1.0.34 KiroIDE-1.4.121-machine-123") {
		t.Fatalf("expected x-amz-user-agent to include version and machine id, got %q", values.AmzUserAgent)
	}
}

func TestBuildRuntimeHeaderValuesUsesRuntimeAPIFormat(t *testing.T) {
	account := &config.Account{MachineId: "machine-456"}
	values := buildRuntimeHeaderValues(account, "codewhisperer.us-east-1.amazonaws.com")

	if !strings.Contains(values.UserAgent, "aws-sdk-js/1.0.0") {
		t.Fatalf("expected runtime sdk version in user agent, got %q", values.UserAgent)
	}
	if !strings.Contains(values.UserAgent, "api/codewhispererruntime#1.0.0") {
		t.Fatalf("expected runtime API marker in user agent, got %q", values.UserAgent)
	}
	if !strings.Contains(values.UserAgent, "m/N,E") {
		t.Fatalf("expected runtime mode marker in user agent, got %q", values.UserAgent)
	}
}

func TestApplyKiroBaseHeadersMarksExternalIdentityProviderTokens(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://q.us-east-1.amazonaws.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	account := &config.Account{
		AccessToken: "external-access",
		AuthMethod:  " External_IDP ",
	}

	applyKiroBaseHeaders(req, account, buildRuntimeHeaderValues(account, req.URL.Host))

	if got := req.Header.Get("Authorization"); got != "Bearer external-access" {
		t.Fatalf("expected bearer authorization, got %q", got)
	}
	if got := req.Header.Get("TokenType"); got != "EXTERNAL_IDP" {
		t.Fatalf("expected EXTERNAL_IDP token type, got %q", got)
	}
}

func TestApplyKiroBaseHeadersOmitsTokenTypeForAWSAuthentication(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://q.us-east-1.amazonaws.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("TokenType", "stale")
	account := &config.Account{AccessToken: "aws-access", AuthMethod: "idc"}

	applyKiroBaseHeaders(req, account, buildRuntimeHeaderValues(account, req.URL.Host))

	if got := req.Header.Get("TokenType"); got != "" {
		t.Fatalf("expected no token type for AWS auth, got %q", got)
	}
}

func TestApplyKiroBaseHeadersMarksAPIKeyCredentials(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://runtime.us-east-1.kiro.dev/", nil)
	if err != nil {
		t.Fatal(err)
	}
	account := &config.Account{
		KiroApiKey:  "ksk_test_key",
		AccessToken: "should-not-win",
		AuthMethod:  "api_key",
	}

	applyKiroBaseHeaders(req, account, buildStreamingHeaderValues(account, req.URL.Host))

	if got := req.Header.Get("Authorization"); got != "Bearer ksk_test_key" {
		t.Fatalf("expected API key bearer, got %q", got)
	}
	// net/http.Header is case-insensitive; either casing maps to the same entry.
	if got := req.Header.Get("tokentype"); got != "API_KEY" {
		t.Fatalf("expected tokentype API_KEY, got %q", got)
	}
	if got := req.Header.Get("TokenType"); got != "API_KEY" {
		t.Fatalf("expected TokenType/tokentype API_KEY, got %q", got)
	}
}
