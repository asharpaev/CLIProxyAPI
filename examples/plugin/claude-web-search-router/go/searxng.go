package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type searxngClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func newSearXNGClient(baseURL, apiKey string) *searxngClient {
	return newSearXNGClientWithOptions(baseURL, apiKey, nil)
}

func newSearXNGClientWithOptions(baseURL, apiKey string, httpClient *http.Client) *searxngClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &searxngClient{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		apiKey:  strings.TrimSpace(apiKey),
		http:    httpClient,
	}
}

func (c *searxngClient) available() bool {
	return c != nil && c.baseURL != ""
}

type searxngSearchResponse struct {
	Results []struct {
		Title   string `json:"title"`
		URL     string `json:"url"`
		Content string `json:"content"`
	} `json:"results"`
}

func (c *searxngClient) search(ctx context.Context, query string, maxResults int) ([]claudeWebSearchHit, string, error) {
	if !c.available() {
		return nil, "", fmt.Errorf("searxng_url is empty")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, "", fmt.Errorf("web search query is empty")
	}
	if maxResults <= 0 {
		maxResults = 5
	}

	params := url.Values{}
	params.Set("q", query)
	params.Set("format", "json")
	params.Set("pageno", "1")
	req, errNew := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/search?"+params.Encode(), nil)
	if errNew != nil {
		return nil, "", errNew
	}
	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, errDo := c.http.Do(req)
	if errDo != nil {
		return nil, "", errDo
	}
	defer func() { _ = resp.Body.Close() }()
	body, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return nil, "", errRead
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("searxng http %d: %s", resp.StatusCode, truncate(string(body), 512))
	}
	var parsed searxngSearchResponse
	if errDecode := json.Unmarshal(body, &parsed); errDecode != nil {
		return nil, "", errDecode
	}

	hits := make([]claudeWebSearchHit, 0, maxResults)
	for _, r := range parsed.Results {
		rURL := strings.TrimSpace(r.URL)
		if rURL == "" {
			continue
		}
		hits = append(hits, claudeWebSearchHit{
			Title:   strings.TrimSpace(r.Title),
			URL:     rURL,
			Snippet: strings.TrimSpace(r.Content),
		})
		if len(hits) >= maxResults {
			break
		}
	}
	// SearXNG has no synthesized answer field; return empty so the Claude response
	// builder omits the answer text block.
	return hits, "", nil
}
