//
// Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestMaydayConnectorConfigDefaults(t *testing.T) {
	conn, err := NewMaydayConnector(map[string]any{
		"credentials": map[string]any{
			"client_id":     "my_client_id",
			"client_secret": "my_client_secret",
		},
	})
	if err != nil {
		t.Fatalf("NewMaydayConnector error: %v", err)
	}

	if conn.clientID != "my_client_id" {
		t.Errorf("expected clientID 'my_client_id', got %q", conn.clientID)
	}
	if conn.clientSecret != "my_client_secret" {
		t.Errorf("expected clientSecret 'my_client_secret', got %q", conn.clientSecret)
	}
	if conn.baseURL != defaultMaydayAPIBaseURL {
		t.Errorf("expected default base URL %q, got %q", defaultMaydayAPIBaseURL, conn.baseURL)
	}
	if conn.tokenURL != defaultMaydayOAuthURL {
		t.Errorf("expected default token URL %q, got %q", defaultMaydayOAuthURL, conn.tokenURL)
	}
	if conn.batchSize != defaultMaydayBatchSize {
		t.Errorf("expected batch size %d, got %d", defaultMaydayBatchSize, conn.batchSize)
	}
	if conn.includeDrafts {
		t.Errorf("expected includeDrafts=false by default")
	}
}

func TestMaydayConnectorCustomConfigAndFilters(t *testing.T) {
	conn, err := NewMaydayConnector(map[string]any{
		"client_id":      "custom_id",
		"client_secret":  "custom_secret",
		"base_url":       "https://api.mayday.fr/v2/",
		"token_url":      "https://api.mayday.fr/oauth/v2/token/",
		"batch_size":     15,
		"collections":    "billing, technical, HR",
		"languages":      []string{"FR", "EN"},
		"include_drafts": "true",
	})
	if err != nil {
		t.Fatalf("NewMaydayConnector error: %v", err)
	}

	if conn.baseURL != "https://api.mayday.fr/v2" {
		t.Errorf("expected trimmed base URL, got %q", conn.baseURL)
	}
	if conn.tokenURL != "https://api.mayday.fr/oauth/v2/token" {
		t.Errorf("expected trimmed token URL, got %q", conn.tokenURL)
	}
	if conn.batchSize != 15 {
		t.Errorf("expected batch size 15, got %d", conn.batchSize)
	}
	if !conn.includeDrafts {
		t.Errorf("expected includeDrafts=true")
	}
	if len(conn.collections) != 3 {
		t.Errorf("expected 3 collections, got %d", len(conn.collections))
	}
	if _, ok := conn.collections["billing"]; !ok {
		t.Errorf("expected 'billing' in collections")
	}
	if len(conn.languages) != 2 {
		t.Errorf("expected 2 languages, got %d", len(conn.languages))
	}
	if _, ok := conn.languages["fr"]; !ok {
		t.Errorf("expected 'fr' in languages")
	}
}

func TestMaydayConnectorValidationMissingCredentials(t *testing.T) {
	conn, err := NewMaydayConnector(map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = conn.Validate(context.Background())
	if err == nil {
		t.Fatal("expected error for missing credentials, got nil")
	}
	var credErr *ConnectorMissingCredentialError
	if !errors.As(err, &credErr) {
		t.Errorf("expected ConnectorMissingCredentialError, got %T: %v", err, err)
	}
}

func TestMaydayConnectorOAuthTokenAcquisitionAndRefresh(t *testing.T) {
	var tokenCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			atomic.AddInt32(&tokenCalls, 1)
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if err := r.ParseForm(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if r.FormValue("grant_type") != "client_credentials" ||
				r.FormValue("client_id") != "test_id" ||
				r.FormValue("client_secret") != "test_secret" {
				http.Error(w, `{"error":"invalid_client","error_description":"Bad credentials"}`, http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "token_12345",
				"token_type":   "Bearer",
				"expires_in":   2, // 2 seconds
			})
		case "/v1/articles":
			auth := r.Header.Get("Authorization")
			if auth != "Bearer token_12345" {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{
						"id":         "art_1",
						"title":      "Getting Started",
						"content":    "<p>Welcome to Mayday!</p>",
						"status":     "published",
						"updated_at": "2026-04-01T10:00:00Z",
					},
				},
				"has_more": false,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	conn, err := NewMaydayConnector(map[string]any{
		"client_id":     "test_id",
		"client_secret": "test_secret",
		"base_url":      server.URL + "/v1",
		"token_url":     server.URL + "/oauth/token",
	})
	if err != nil {
		t.Fatalf("NewMaydayConnector: %v", err)
	}

	ctx := context.Background()
	// 1. Validation should acquire token and ping articles
	if err := conn.Validate(ctx); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
	if atomic.LoadInt32(&tokenCalls) != 1 {
		t.Fatalf("expected 1 token call, got %d", tokenCalls)
	}

	// 2. Immediate reuse should hit token cache
	token, err := conn.getOrRefreshToken(ctx)
	if err != nil || token != "token_12345" {
		t.Fatalf("expected cached token, got %q, err=%v", token, err)
	}
	if atomic.LoadInt32(&tokenCalls) != 1 {
		t.Fatalf("token was not cached; calls = %d", tokenCalls)
	}

	// 3. Fast-forward simulated time past expiry
	conn.now = func() time.Time {
		return time.Now().Add(5 * time.Second)
	}
	token, err = conn.getOrRefreshToken(ctx)
	if err != nil || token != "token_12345" {
		t.Fatalf("expected refreshed token, got %q, err=%v", token, err)
	}
	if atomic.LoadInt32(&tokenCalls) != 2 {
		t.Fatalf("expected 2 token calls after expiry, got %d", tokenCalls)
	}
}

func TestMaydayConnectorOpenSyncAndPrune(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "valid_token",
				"token_type":   "Bearer",
				"expires_in":   3600,
			})
		case "/v1/collections":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": "col_billing", "name": "Billing & Subscriptions"},
					{"id": "col_support", "name": "Customer Support"},
				},
			})
		case "/v1/articles":
			query := r.URL.Query()
			page := query.Get("page")
			w.Header().Set("Content-Type", "application/json")
			if page == "1" || page == "" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{
						{
							"id":            "art_101",
							"title":         "Invoice FAQs",
							"content":       "<h1>Invoices</h1><p>How to download your monthly invoice.</p>",
							"collection_id": "col_billing",
							"language":      "en",
							"tags":          []string{"invoices", "pdf"},
							"status":        "published",
							"updated_at":    "2026-03-15T12:00:00Z",
						},
						{
							"id":            "art_102",
							"title":         "Internal Draft Article",
							"content":       "<p>Internal notes.</p>",
							"collection_id": "col_support",
							"status":        "draft",
							"updated_at":    "2026-03-16T12:00:00Z",
						},
					},
					"page":        1,
					"total_pages": 2,
					"has_more":    true,
				})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{
						{
							"id":            "art_103",
							"title":         "Contact Support",
							"content":       "<p>Reach us at support@example.com</p>",
							"collection_id": "col_support",
							"language":      "en",
							"tags":          []string{"contact", "email"},
							"status":        "published",
							"updated_at":    "2026-03-17T12:00:00Z",
						},
					},
					"page":        2,
					"total_pages": 2,
					"has_more":    false,
				})
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	conn, err := NewMaydayConnector(map[string]any{
		"client_id":      "client",
		"client_secret":  "secret",
		"base_url":       server.URL + "/v1",
		"token_url":      server.URL + "/oauth/token",
		"batch_size":     10,
		"include_drafts": false, // art_102 should be omitted
	})
	if err != nil {
		t.Fatalf("NewMaydayConnector: %v", err)
	}

	ctx := context.Background()

	// Test OpenSync
	syncSess, err := conn.OpenSync(ctx, SyncRequest{FromBeginning: true})
	if err != nil {
		t.Fatalf("OpenSync: %v", err)
	}
	defer syncSess.Close()

	batch1, err := syncSess.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}

	// Expected: art_101 and art_103 (draft art_102 excluded)
	if len(batch1.Documents) != 2 {
		t.Fatalf("expected 2 documents across pages, got %d", len(batch1.Documents))
	}

	doc1 := batch1.Documents[0]
	if doc1.SourceID != "mayday_article_art_101" {
		t.Errorf("expected sourceID 'mayday_article_art_101', got %q", doc1.SourceID)
	}
	if doc1.SemanticIdentifier != "Invoice FAQs" {
		t.Errorf("expected semantic identifier 'Invoice FAQs', got %q", doc1.SemanticIdentifier)
	}
	if doc1.Metadata["collection_name"] != "Billing & Subscriptions" {
		t.Errorf("expected collection name enriched, got %v", doc1.Metadata["collection_name"])
	}

	// Verify next batch returns EOF
	_, err = syncSess.NextBatch(ctx)
	if err != io.EOF {
		t.Fatalf("expected EOF on final batch, got %v", err)
	}

	// Test OpenPrune
	pruneSess, err := conn.OpenPrune(ctx, PruneRequest{})
	if err != nil {
		t.Fatalf("OpenPrune: %v", err)
	}
	defer pruneSess.Close()

	pruneBatch, err := pruneSess.NextBatch(ctx)
	if err != nil {
		t.Fatalf("Prune NextBatch: %v", err)
	}
	// Prune emits all article IDs (3 articles total)
	if len(pruneBatch.Documents) != 3 {
		t.Fatalf("expected 3 slim documents in prune, got %d", len(pruneBatch.Documents))
	}
}

func TestMaydayConnectorCheckpointResume(t *testing.T) {
	conn := &MaydayConnector{
		clientID:     "test",
		clientSecret: "test",
		baseURL:      "https://api.mayday.fr/v1",
		tokenURL:     "https://api.mayday.fr/oauth/token",
		now:          time.Now,
	}

	session := &maydaySyncSession{
		connector:   conn,
		request:     SyncRequest{},
		currentPage: 1,
	}

	// Valid resume checkpoint
	cursorJSON, _ := json.Marshal(maydaySyncCursor{
		Page:     2,
		SourceID: "mayday_article_123",
	})
	session.request.Resume = &SyncCheckpoint{
		Cursor:   string(cursorJSON),
		SourceID: "mayday_article_123",
	}

	if err := session.validateResume(context.Background()); err != nil {
		t.Fatalf("validateResume error: %v", err)
	}
	if session.currentPage != 2 {
		t.Errorf("expected resume page 2, got %d", session.currentPage)
	}
	if session.resumeSource != "mayday_article_123" {
		t.Errorf("expected resume source 'mayday_article_123', got %q", session.resumeSource)
	}
	if !session.resumeSkip {
		t.Errorf("expected resumeSkip to be true")
	}

	// Invalid anchor format should return ErrSyncResumeInvalid
	badSession := &maydaySyncSession{
		connector: conn,
		request: SyncRequest{
			Resume: &SyncCheckpoint{
				Cursor:   `{"source_id":"not_a_mayday_anchor"}`,
				SourceID: "not_a_mayday_anchor",
			},
		},
	}
	err := badSession.validateResume(context.Background())
	if err == nil || !errors.Is(err, ErrSyncResumeInvalid) {
		t.Fatalf("expected ErrSyncResumeInvalid, got %v", err)
	}
}
