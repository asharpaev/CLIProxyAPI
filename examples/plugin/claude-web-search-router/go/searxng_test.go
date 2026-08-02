package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/tidwall/gjson"
)

func TestSearXNGClientSearchMockAPI(t *testing.T) {
	var gotQuery string
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if ct := r.Header.Get("Accept"); !strings.Contains(ct, "application/json") {
			t.Errorf("accept = %q", ct)
		}
		gotQuery = r.URL.Query().Get("q")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"query": "北京天气",
			"results": [
				{"title": "Example Weather", "url": "https://example.com/w", "content": "snippet one"},
				{"title": "No URL result", "url": "", "content": "dropped"},
				{"title": "Second", "url": "https://example.com/x", "content": "snippet two"}
			]
		}`))
	}))
	defer server.Close()

	client := newSearXNGClientWithOptions(server.URL, "bearer-secret", server.Client())
	hits, answer, errSearch := client.search(context.Background(), "北京天气", 2)
	if errSearch != nil {
		t.Fatalf("search() error = %v", errSearch)
	}
	if gotQuery != "北京天气" {
		t.Fatalf("query = %q", gotQuery)
	}
	if gotAuth != "Bearer bearer-secret" {
		t.Fatalf("auth = %q", gotAuth)
	}
	if answer != "" {
		t.Fatalf("answer should be empty, got %q", answer)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %#v (want 2: no-URL dropped, then capped at maxResults)", hits)
	}
	if hits[0].URL != "https://example.com/w" {
		t.Fatalf("hits[0].URL = %q", hits[0].URL)
	}
}

func TestSearXNGClientSearchNoAPIKey(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := newSearXNGClientWithOptions(server.URL, "", server.Client())
	if _, _, err := client.search(context.Background(), "q", 5); err != nil {
		t.Fatalf("err = %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization header should be absent, got %q", gotAuth)
	}
}

func TestSearXNGClientSearchEmptyURL(t *testing.T) {
	client := newSearXNGClient("", "")
	_, _, err := client.search(context.Background(), "q", 5)
	if err == nil || !strings.Contains(err.Error(), "searxng_url") {
		t.Fatalf("err = %v", err)
	}
}

func TestSearXNGClientSearchHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"offline"}`))
	}))
	defer server.Close()
	client := newSearXNGClientWithOptions(server.URL, "", server.Client())
	_, _, err := client.search(context.Background(), "q", 5)
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err = %v", err)
	}
}

func TestSearXNGClientDropsEmptyURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"results":[{"title":"a","url":"https://a.example","content":"x"},{"title":"b","url":"","content":"y"}]}`))
	}))
	defer server.Close()
	client := newSearXNGClientWithOptions(server.URL, "", server.Client())
	hits, _, err := client.search(context.Background(), "q", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("hits = %#v (want empty-URL filtered)", hits)
	}
}

func TestRunSearXNGClaudeStreamWithMock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"results": [
				{"title": "bjmy.gov.cn", "url": "https://www.bjmy.gov.cn/x", "content": "预报"}
			]
		}`))
	}))
	defer server.Close()

	claudeBody := []byte(`{
		"model": "claude-sonnet-4-6",
		"stream": true,
		"tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}],
		"messages": [{"role": "user", "content": [{"type": "text", "text": "Perform a web search for the query: 北京天气 2026年6月16日"}]}]
	}`)
	client := newSearXNGClientWithOptions(server.URL, "", server.Client())
	payload, headers, errRun := runSearXNGClaudeStreamWithClient(context.Background(), pluginapi.ExecutorRequest{
		Model:           "claude-sonnet-4-6",
		Stream:          true,
		OriginalRequest: claudeBody,
	}, client)
	if errRun != nil {
		t.Fatalf("runSearXNGClaudeStreamWithClient() error = %v", errRun)
	}
	if headers.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content-type = %q", headers.Get("Content-Type"))
	}
	text := string(payload)
	for _, needle := range []string{
		"event: message_start",
		`"type":"server_tool_use"`,
		`"name":"web_search"`,
		`"type":"web_search_tool_result"`,
		`"type":"web_search_result"`,
		`https://www.bjmy.gov.cn/x`,
		`"web_search_requests":1`,
		"event: message_stop",
		"北京天气 2026年6月16日",
	} {
		if !strings.Contains(text, needle) {
			t.Fatalf("SSE missing %q in:\n%s", needle, text)
		}
	}
}

func TestRunSearXNGClaudeJSONWithMock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"results":[{"title":"T","url":"https://t.example","content":"c"}]}`))
	}))
	defer server.Close()

	claudeBody := []byte(`{
		"tools": [{"type": "web_search_20250305", "name": "web_search"}],
		"messages": [{"role": "user", "content": "Perform a web search for the query: test query"}]
	}`)
	client := newSearXNGClientWithOptions(server.URL, "", server.Client())
	payload, _, errRun := runSearXNGClaudeWithClient(context.Background(), pluginapi.ExecutorRequest{
		Model:           "claude-sonnet-4-6",
		OriginalRequest: claudeBody,
	}, client)
	if errRun != nil {
		t.Fatal(errRun)
	}
	root := gjson.ParseBytes(payload)
	if root.Get("type").String() != "message" {
		t.Fatalf("type = %s", root.Get("type").String())
	}
	if root.Get("content.0.type").String() != "server_tool_use" {
		t.Fatalf("content.0 = %s", root.Get("content.0.type").String())
	}
	if root.Get("content.1.type").String() != "web_search_tool_result" {
		t.Fatalf("content.1 = %s", root.Get("content.1.type").String())
	}
	if root.Get("usage.server_tool_use.web_search_requests").Int() != 1 {
		t.Fatalf("web_search_requests = %d", root.Get("usage.server_tool_use.web_search_requests").Int())
	}
}
