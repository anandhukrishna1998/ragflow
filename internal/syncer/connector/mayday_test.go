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
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

const (
	maydayBillingCollectionID = "64b7f0c2a1b2c3d4e5f60718"
	maydaySupportCollectionID = "64b7f0c2a1b2c3d4e5f60719"
	maydayHelpCollectionID    = "64b7f0c2a1b2c3d4e5f6071a"
)

var maydayTestTime = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// maydayFakeServer mimics the Mayday Public API: Basic-auth client credentials
// grant, bearer-protected list endpoints with updatedAt sorting and filters,
// offset pages, and a server-side page size cap.
type maydayFakeServer struct {
	*httptest.Server
	tokenCalls atomic.Int32

	mu              sync.Mutex
	pageSize        int
	forbidContents  bool
	rejectBearer    bool
	hideCap         bool // echo the requested limit instead of the capped one
	reverseOrder    bool // ignore the requested sort direction
	rotateTies      int  // rotate tie listings by this much more on each request
	tieRequests     int
	inflateTotal    int // add to every reported total, as a count racing a delete would
	tokenFailures   []int
	badRequest      map[string]bool // locales answered with HTTP 400
	forbidLocale    map[string]bool // locales answered with HTTP 403
	emptyProbes     map[string]int  // one-item probes of a locale answered empty, this many times
	contentFailures []int
	onContents      func(query url.Values) // runs after each contents page, under mu
	valid           string
	contents        map[string][]map[string]any
	collections     map[string][]map[string]any
	requests        []url.Values
}

func newMaydayFakeServer(t *testing.T) *maydayFakeServer {
	t.Helper()
	origLoopback, origDelay := connectorAllowLoopbackForTest, maydayRetryBaseDelay
	connectorAllowLoopbackForTest, maydayRetryBaseDelay = true, time.Millisecond
	s := &maydayFakeServer{
		pageSize:    maydayPageSize,
		contents:    map[string][]map[string]any{},
		collections: map[string][]map[string]any{},
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(func() {
		s.Close()
		connectorAllowLoopbackForTest, maydayRetryBaseDelay = origLoopback, origDelay
	})
	return s
}

func (s *maydayFakeServer) connector(t *testing.T, config map[string]any) *MaydayConnector {
	t.Helper()
	merged := map[string]any{
		"languages":   "fr-fr",
		"base_url":    s.URL,
		"token_url":   s.URL + "/auth/oauth2/token",
		"credentials": map[string]any{"client_id": "client", "client_secret": "secret"},
	}
	maps.Copy(merged, config)
	conn, err := NewMaydayConnector(merged)
	if err != nil {
		t.Fatalf("NewMaydayConnector: %v", err)
	}
	return conn
}

func (s *maydayFakeServer) set(update func(s *maydayFakeServer)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	update(s)
}

// listingRequests returns the contents requests other than one-item probes.
func (s *maydayFakeServer) listingRequests() []url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []url.Values
	for _, query := range s.requests {
		if query.Get("limit") != "1" {
			out = append(out, query)
		}
	}
	return out
}

func (s *maydayFakeServer) probeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, query := range s.requests {
		if query.Get("limit") == "1" {
			count++
		}
	}
	return count
}

func (s *maydayFakeServer) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/auth/oauth2/token":
		if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "client_credentials" {
			http.Error(w, `{"message":"Invalid grant type"}`, http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		if len(s.tokenFailures) > 0 {
			status := s.tokenFailures[0]
			s.tokenFailures = s.tokenFailures[1:]
			s.mu.Unlock()
			http.Error(w, `{"message":"try again"}`, status)
			return
		}
		s.mu.Unlock()
		if id, secret, ok := r.BasicAuth(); !ok || id != "client" || secret != "secret" {
			http.Error(w, `{"message":"Invalid client credentials"}`, http.StatusUnauthorized)
			return
		}
		token := fmt.Sprintf("token-%d", s.tokenCalls.Add(1))
		s.set(func(s *maydayFakeServer) { s.valid = token })
		writeMaydayJSON(w, map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 3600})
	case r.Method == http.MethodGet && (r.URL.Path == "/contents" || r.URL.Path == "/collections"):
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.rejectBearer || r.Header.Get("Authorization") != "Bearer "+s.valid {
			http.Error(w, `{"message":"Invalid token"}`, http.StatusUnauthorized)
			return
		}
		query := r.URL.Query()
		if r.URL.Path == "/collections" {
			s.writePage(w, query, s.collections[query.Get("locale")])
			return
		}
		s.requests = append(s.requests, query)
		if len(s.contentFailures) > 0 {
			status := s.contentFailures[0]
			s.contentFailures = s.contentFailures[1:]
			w.Header().Set("Retry-After", "0")
			http.Error(w, `{"message":"try again"}`, status)
			return
		}
		if s.forbidContents {
			http.Error(w, `{"message":"Forbidden"}`, http.StatusForbidden)
			return
		}
		if s.forbidLocale[query.Get("locale")] {
			http.Error(w, `{"message":"Forbidden"}`, http.StatusForbidden)
			return
		}
		if remaining := s.emptyProbes[query.Get("locale")]; remaining > 0 && query.Get("limit") == "1" {
			s.emptyProbes[query.Get("locale")] = remaining - 1
			s.writePage(w, query, nil)
			return
		}
		if s.badRequest[query.Get("locale")] {
			http.Error(w, `{"message":["locale must be one of the following values"]}`, http.StatusBadRequest)
			return
		}
		items, err := s.filterContents(query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if s.rotateTies > 0 && query.Get("updatedAt[gte]") != "" && query.Get("updatedAt[gte]") == query.Get("updatedAt[lte]") && len(items) > 0 {
			s.tieRequests++
			shift := (s.tieRequests * s.rotateTies) % len(items)
			items = append(items[shift:], items[:shift]...)
		}
		s.writePage(w, query, items)
		if s.onContents != nil {
			s.onContents(query)
		}
	default:
		http.NotFound(w, r)
	}
}

func (s *maydayFakeServer) filterContents(query url.Values) ([]map[string]any, error) {
	bounds := map[string]int64{}
	for _, op := range []string{"gt", "gte", "lte"} {
		raw := query.Get("updatedAt[" + op + "]")
		if raw == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return nil, err
		}
		bounds[op] = parsed.UnixMilli()
	}
	collectionIDs := query["collectionIds[]"]
	var out []map[string]any
	for _, item := range s.contents[query.Get("locale")] {
		updatedAt := item["updatedAt"].(int64)
		if gt, ok := bounds["gt"]; ok && updatedAt <= gt {
			continue
		}
		if gte, ok := bounds["gte"]; ok && updatedAt < gte {
			continue
		}
		if lte, ok := bounds["lte"]; ok && updatedAt > lte {
			continue
		}
		if query.Get("types[]") != "" && item["type"] != query.Get("types[]") {
			continue
		}
		if query.Get("isPublished") == "true" && item["isPublished"] != true {
			continue
		}
		if len(collectionIDs) > 0 && !slices.ContainsFunc(item["collectionsIds"].([]string), func(id string) bool {
			return slices.Contains(collectionIDs, id)
		}) {
			continue
		}
		out = append(out, item)
	}
	if query.Get("sortKey") == "updatedAt" {
		sort.SliceStable(out, func(i, j int) bool { return out[i]["updatedAt"].(int64) < out[j]["updatedAt"].(int64) })
		if s.reverseOrder {
			slices.Reverse(out)
		}
	}
	return out, nil
}

func (s *maydayFakeServer) writePage(w http.ResponseWriter, query url.Values, items []map[string]any) {
	page, _ := strconv.Atoi(query.Get("page"))
	limit, _ := strconv.Atoi(query.Get("limit"))
	page = max(page, 1)
	limit = min(max(limit, 1), s.pageSize)
	start := min((page-1)*limit, len(items))
	end := min(start+limit, len(items))
	data := append([]map[string]any{}, items[start:end]...)
	reported := limit
	if s.hideCap {
		reported, _ = strconv.Atoi(query.Get("limit"))
	}
	writeMaydayJSON(w, map[string]any{"data": data, "total": len(items) + s.inflateTotal, "page": page, "limit": reported})
}

func writeMaydayJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func maydayTestContent(id, label, body string, updatedAt time.Time, published bool, collectionIDs ...string) map[string]any {
	return map[string]any{
		"id":             id,
		"selfcareId":     "sc_" + id,
		"knowledgeId":    "kb_1",
		"label":          label,
		"body":           body,
		"type":           "Article",
		"isPublished":    published,
		"collectionsIds": collectionIDs,
		"isOutdated":     false,
		"updatedAt":      updatedAt.UnixMilli(),
		"updatedAtISO":   updatedAt.Format(time.RFC3339),
	}
}

// maydayArticles builds published articles named prefix1..prefixN, updated one
// minute apart.
func maydayArticles(prefix string, count int) []map[string]any {
	items := make([]map[string]any, 0, count)
	for i := 1; i <= count; i++ {
		id := fmt.Sprintf("%s%d", prefix, i)
		items = append(items, maydayTestContent(id, id, "<p>"+id+"</p>", maydayTestTime.Add(time.Duration(i)*time.Minute), true))
	}
	return items
}

func maydayIDs(locale string, contentIDs ...string) []string {
	ids := make([]string, 0, len(contentIDs))
	for _, id := range contentIDs {
		ids = append(ids, maydaySourceID(locale, id))
	}
	return ids
}

func drainMaydaySync(t *testing.T, session SyncSession) []SourceDocument {
	t.Helper()
	var docs []SourceDocument
	for {
		batch, err := session.NextBatch(context.Background())
		if errors.Is(err, io.EOF) {
			return docs
		}
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if batch.Checkpoint == nil {
			t.Fatalf("batch without checkpoint")
		}
		docs = append(docs, batch.Documents...)
	}
}

func drainMaydayPrune(t *testing.T, session PruneSession) []string {
	t.Helper()
	var ids []string
	for {
		batch, err := session.NextBatch(context.Background())
		if errors.Is(err, io.EOF) {
			return ids
		}
		if err != nil {
			t.Fatalf("prune NextBatch: %v", err)
		}
		for _, doc := range batch.Documents {
			ids = append(ids, doc.SourceID)
		}
	}
}

func syncMaydayIDs(t *testing.T, conn *MaydayConnector, request SyncRequest) []string {
	t.Helper()
	session, err := conn.OpenSync(context.Background(), request)
	if err != nil {
		t.Fatalf("OpenSync: %v", err)
	}
	defer session.Close()
	return maydayDocIDs(drainMaydaySync(t, session))
}

func pruneMaydayIDs(t *testing.T, conn *MaydayConnector) []string {
	t.Helper()
	session, err := conn.OpenPrune(context.Background(), PruneRequest{})
	if err != nil {
		t.Fatalf("OpenPrune: %v", err)
	}
	defer session.Close()
	return drainMaydayPrune(t, session)
}

func maydayDocIDs(docs []SourceDocument) []string {
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.SourceID)
	}
	return ids
}

func TestMaydayConnectorConfig(t *testing.T) {
	conn, err := NewMaydayConnector(map[string]any{
		"credentials": map[string]any{"client_id": " id ", "client_secret": "secret"},
	})
	if err != nil {
		t.Fatalf("NewMaydayConnector: %v", err)
	}
	if conn.clientID != "id" || conn.clientSecret != "secret" {
		t.Fatalf("credentials = %q/%q", conn.clientID, conn.clientSecret)
	}
	if conn.baseURL != defaultMaydayAPIBaseURL || conn.tokenURL != defaultMaydayTokenURL || conn.batchSize != defaultMaydayBatchSize {
		t.Fatalf("baseURL=%q tokenURL=%q batchSize=%d", conn.baseURL, conn.tokenURL, conn.batchSize)
	}
	if len(conn.locales) != 0 || len(conn.collections) != 0 {
		t.Fatalf("locales=%v collections=%v, want empty", conn.locales, conn.collections)
	}

	conn, err = NewMaydayConnector(map[string]any{
		"credentials": map[string]any{"client_id": "id", "client_secret": "secret"},
		"base_url":    "https://mayday.example.com/api/",
		"batch_size":  "15",
		"languages":   "FR_fr, en-GB, fr-fr",
		"collections": []any{"Billing", " " + maydayBillingCollectionID + " ", "Billing"},
	})
	if err != nil {
		t.Fatalf("NewMaydayConnector: %v", err)
	}
	if conn.baseURL != "https://mayday.example.com/api" || conn.batchSize != 15 {
		t.Fatalf("baseURL=%q batchSize=%d", conn.baseURL, conn.batchSize)
	}
	if !slices.Equal(conn.locales, []string{"fr-fr", "en-gb"}) {
		t.Fatalf("locales = %v", conn.locales)
	}
	if !slices.Equal(conn.collections, []string{"Billing", maydayBillingCollectionID}) {
		t.Fatalf("collections = %v", conn.collections)
	}
}

func TestMaydayConnectorRegisteredBuiltIn(t *testing.T) {
	registry := NewRegistry()
	RegisterBuiltIns(registry)
	conn, err := registry.OpenFromConfig("mayday", map[string]any{
		"credentials": map[string]any{"client_id": "id", "client_secret": "secret"},
	})
	if err != nil {
		t.Fatalf("OpenFromConfig: %v", err)
	}
	if _, ok := conn.(*MaydayConnector); !ok {
		t.Fatalf("connector type = %T, want *MaydayConnector", conn)
	}
	if _, ok := conn.(SettingValidator); !ok {
		t.Fatalf("MaydayConnector does not implement SettingValidator")
	}
}

func TestMaydayConnectorValidateRejectsInvalidConfig(t *testing.T) {
	credentials := map[string]any{"client_id": "id", "client_secret": "secret"}
	cases := []struct {
		name       string
		config     map[string]any
		credential bool
	}{
		{"missing credentials", map[string]any{}, true},
		{"unsupported language", map[string]any{"credentials": credentials, "languages": "fr"}, false},
		{"default locale", map[string]any{"credentials": credentials, "languages": "default"}, false},
		{"plain http", map[string]any{"credentials": credentials, "base_url": "http://public-api.mayday.fr"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := NewMaydayConnector(tc.config)
			if err != nil {
				t.Fatalf("NewMaydayConnector: %v", err)
			}
			err = conn.Validate(context.Background())
			var credErr *ConnectorMissingCredentialError
			var valErr *ConnectorValidationError
			if tc.credential && !errors.As(err, &credErr) {
				t.Fatalf("Validate error = %T %v, want ConnectorMissingCredentialError", err, err)
			}
			if !tc.credential && !errors.As(err, &valErr) {
				t.Fatalf("Validate error = %T %v, want ConnectorValidationError", err, err)
			}
		})
	}
}

func TestMaydayConnectorValidateCachesAndRefreshesToken(t *testing.T) {
	srv := newMaydayFakeServer(t)
	conn := srv.connector(t, nil)
	now := maydayTestTime
	conn.now = func() time.Time { return now }
	ctx := context.Background()

	for range 2 {
		if err := conn.Validate(ctx); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	}
	if got := srv.tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want the cached token reused", got)
	}
	var probe url.Values
	srv.set(func(s *maydayFakeServer) { probe = s.requests[0] })
	if probe.Get("locale") != "fr-fr" || probe.Get("types[]") != "Article" || probe.Get("limit") != "1" {
		t.Fatalf("validation probe query = %v", probe)
	}

	now = now.Add(time.Hour)
	if err := conn.Validate(ctx); err != nil {
		t.Fatalf("Validate after expiry: %v", err)
	}
	if got := srv.tokenCalls.Load(); got != 2 {
		t.Fatalf("token calls = %d, want a refresh after expiry", got)
	}

	srv.set(func(s *maydayFakeServer) { s.valid = "" })
	if err := conn.Validate(ctx); err != nil {
		t.Fatalf("Validate after revocation: %v", err)
	}
	if got := srv.tokenCalls.Load(); got != 3 {
		t.Fatalf("token calls = %d, want a refresh after HTTP 401", got)
	}
}

func TestMaydayConnectorValidateConnectorSettingErrors(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.collections["fr-fr"] = []map[string]any{{"id": maydayBillingCollectionID, "label": "Billing"}}
	srv.contents["fr-fr"] = []map[string]any{maydayTestContent("a1", "Factures", "<p>Factures</p>", maydayTestTime, true, maydayBillingCollectionID)}
	ctx := context.Background()

	if err := srv.connector(t, map[string]any{"collections": "Billing"}).ValidateConnectorSetting(ctx, nil); err != nil {
		t.Fatalf("ValidateConnectorSetting: %v", err)
	}

	var credErr *ConnectorMissingCredentialError
	var valErr *ConnectorValidationError
	err := srv.connector(t, map[string]any{
		"credentials": map[string]any{"client_id": "client", "client_secret": "wrong"},
	}).ValidateConnectorSetting(ctx, nil)
	if !errors.As(err, &credErr) {
		t.Fatalf("bad secret error = %T %v, want ConnectorMissingCredentialError", err, err)
	}

	err = srv.connector(t, map[string]any{"collections": "Unknown"}).ValidateConnectorSetting(ctx, nil)
	if !errors.As(err, &valErr) || !strings.Contains(err.Error(), "Unknown") {
		t.Fatalf("unknown collection error = %T %v", err, err)
	}

	err = srv.connector(t, map[string]any{"languages": "en-gb"}).ValidateConnectorSetting(ctx, nil)
	if !errors.As(err, &valErr) || !strings.Contains(err.Error(), "select no published articles") {
		t.Fatalf("empty selection error = %T %v", err, err)
	}

	err = srv.connector(t, map[string]any{"base_url": srv.URL + "/wrong"}).ValidateConnectorSetting(ctx, nil)
	if !errors.As(err, &valErr) || !strings.Contains(err.Error(), "HTTP 404") {
		t.Fatalf("not found error = %T %v", err, err)
	}

	srv.set(func(s *maydayFakeServer) {
		s.contentFailures = slices.Repeat([]int{http.StatusTooManyRequests}, maydayMaxRateLimitRetries+1)
	})
	err = srv.connector(t, nil).ValidateConnectorSetting(ctx, nil)
	if !errors.As(err, &valErr) || !strings.Contains(err.Error(), "HTTP 429") {
		t.Fatalf("rate limit error = %T %v", err, err)
	}

	srv.set(func(s *maydayFakeServer) { s.rejectBearer = true })
	err = srv.connector(t, nil).ValidateConnectorSetting(ctx, nil)
	if !errors.As(err, &credErr) {
		t.Fatalf("persistent 401 error = %T %v, want ConnectorMissingCredentialError", err, err)
	}

	srv.set(func(s *maydayFakeServer) { s.rejectBearer, s.forbidContents = false, true })
	err = srv.connector(t, nil).ValidateConnectorSetting(ctx, nil)
	if !errors.As(err, &valErr) || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("forbidden error = %T %v", err, err)
	}
}

func TestMaydayConnectorRetriesUnavailableResponses(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contentFailures = []int{http.StatusTooManyRequests, http.StatusServiceUnavailable}
	if err := srv.connector(t, nil).Validate(context.Background()); err != nil {
		t.Fatalf("Validate after transient failures: %v", err)
	}
	if got := srv.probeCount(); got != 3 {
		t.Fatalf("probe attempts = %d, want 2 retries then success", got)
	}
}

func TestMaydayConnectorSyncDocumentsAcrossLocales(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.pageSize = 2
	srv.contents["fr-fr"] = []map[string]any{
		maydayTestContent("a1", "Invoices", "<p>How to <strong>download</strong> an invoice.</p>", maydayTestTime, true, maydayBillingCollectionID),
		maydayTestContent("a2", "Draft", "<p>Not ready.</p>", maydayTestTime.Add(time.Minute), false),
		maydayTestContent("a3", "Refunds", "<p>Refund policy.</p>", maydayTestTime.Add(2*time.Minute), true),
		maydayTestContent("a4", "Empty", "  ", maydayTestTime.Add(3*time.Minute), true),
		maydayTestContent("a5", "Contact", "<p>Write to us.</p>", maydayTestTime.Add(4*time.Minute), true),
	}
	srv.contents["en-gb"] = []map[string]any{maydayTestContent("b1", "Invoices", "<p>Invoices in English.</p>", maydayTestTime, true)}
	conn := srv.connector(t, map[string]any{"languages": "fr-fr, en-gb", "batch_size": 10})

	session, err := conn.OpenSync(context.Background(), SyncRequest{FromBeginning: true})
	if err != nil {
		t.Fatalf("OpenSync: %v", err)
	}
	docs := drainMaydaySync(t, session)

	want := append(maydayIDs("fr-fr", "a1", "a3", "a5"), maydayIDs("en-gb", "b1")...)
	if got := maydayDocIDs(docs); !slices.Equal(got, want) {
		t.Fatalf("source IDs = %v, want %v", got, want)
	}
	doc := docs[0]
	text := string(doc.Blob)
	if doc.SemanticIdentifier != "Invoices (fr-fr)" || doc.Extension != ".md" {
		t.Fatalf("name = %q%s", doc.SemanticIdentifier, doc.Extension)
	}
	if !strings.HasPrefix(text, "# Invoices\n\n") || !strings.Contains(text, "**download**") {
		t.Fatalf("blob = %q", text)
	}
	if !doc.UpdatedAt.Equal(maydayTestTime) || doc.Fingerprint == "" || doc.SizeBytes != int64(len(doc.Blob)) {
		t.Fatalf("updatedAt=%v fingerprint=%q size=%d", doc.UpdatedAt, doc.Fingerprint, doc.SizeBytes)
	}
	if doc.Metadata["locale"] != "fr-fr" || doc.Metadata["content_id"] != "a1" || doc.Metadata["selfcare_id"] != "sc_a1" {
		t.Fatalf("metadata = %v", doc.Metadata)
	}
	if ids, _ := doc.Metadata["collection_ids"].([]string); !slices.Equal(ids, []string{maydayBillingCollectionID}) {
		t.Fatalf("collection_ids = %v", doc.Metadata["collection_ids"])
	}
	for _, query := range srv.listingRequests() {
		if query.Get("isPublished") != "true" || query.Get("types[]") != "Article" || query.Get("sortKey") != "updatedAt" ||
			query.Get("sortDirection") != "1" || query.Get("limit") != "100" {
			t.Fatalf("contents query = %v", query)
		}
	}
}

func TestMaydayConnectorFingerprintTracksContent(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = []map[string]any{maydayTestContent("a1", "Factures", "<p>v1</p>", maydayTestTime, true)}
	conn := srv.connector(t, nil)
	fingerprint := func() string {
		session, err := conn.OpenSync(context.Background(), SyncRequest{FromBeginning: true})
		if err != nil {
			t.Fatalf("OpenSync: %v", err)
		}
		return drainMaydaySync(t, session)[0].Fingerprint
	}

	first, second := fingerprint(), fingerprint()
	if first != second {
		t.Fatalf("fingerprint changed between identical syncs: %q, %q", first, second)
	}
	srv.set(func(s *maydayFakeServer) { s.contents["fr-fr"][0]["body"] = "<p>v2</p>" })
	if fingerprint() == first {
		t.Fatalf("fingerprint did not change with the article body")
	}
}

func TestMaydayConnectorSyncAppliesWindow(t *testing.T) {
	srv := newMaydayFakeServer(t)
	start, end := maydayTestTime, maydayTestTime.Add(2*time.Hour)
	srv.contents["fr-fr"] = []map[string]any{
		maydayTestContent("at-start", "a", "<p>a</p>", start, true),
		maydayTestContent("inside", "b", "<p>b</p>", start.Add(time.Hour), true),
		maydayTestContent("at-end", "c", "<p>c</p>", end, true),
		maydayTestContent("after-end", "d", "<p>d</p>", end.Add(time.Millisecond), true),
	}

	got := syncMaydayIDs(t, srv.connector(t, nil), SyncRequest{WindowStart: &start, WindowEnd: end})
	if want := maydayIDs("fr-fr", "inside", "at-end"); !slices.Equal(got, want) {
		t.Fatalf("source IDs = %v, want %v (WindowStart < updatedAt <= WindowEnd)", got, want)
	}
	query := srv.listingRequests()[0]
	if query.Get("updatedAt[gt]") != "2026-03-01T12:00:00.000Z" || query.Get("updatedAt[lte]") != "2026-03-01T14:00:00.000Z" {
		t.Fatalf("window filters = gt %q lte %q", query.Get("updatedAt[gt]"), query.Get("updatedAt[lte]"))
	}
}

func TestMaydayConnectorSyncAppliesCollections(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.collections["fr-fr"] = []map[string]any{
		{"id": maydayBillingCollectionID, "label": "Billing"},
		{"id": maydaySupportCollectionID, "label": "Support"},
		{"id": maydayHelpCollectionID, "label": "support"},
	}
	srv.contents["fr-fr"] = []map[string]any{
		maydayTestContent("bill", "a", "<p>a</p>", maydayTestTime, true, maydayBillingCollectionID),
		maydayTestContent("support", "b", "<p>b</p>", maydayTestTime.Add(time.Minute), true, maydaySupportCollectionID),
		maydayTestContent("help", "c", "<p>c</p>", maydayTestTime.Add(2*time.Minute), true, maydayHelpCollectionID),
	}

	if got := syncMaydayIDs(t, srv.connector(t, map[string]any{"collections": maydayBillingCollectionID}), SyncRequest{FromBeginning: true}); !slices.Equal(got, maydayIDs("fr-fr", "bill")) {
		t.Fatalf("collection ID filter = %v", got)
	}
	if got := syncMaydayIDs(t, srv.connector(t, map[string]any{"collections": "SUPPORT"}), SyncRequest{FromBeginning: true}); !slices.Equal(got, maydayIDs("fr-fr", "support", "help")) {
		t.Fatalf("collection name matching two collections = %v", got)
	}
	queries := srv.listingRequests()
	if got := queries[len(queries)-1]["collectionIds[]"]; !slices.Equal(got, []string{maydaySupportCollectionID, maydayHelpCollectionID}) {
		t.Fatalf("collectionIds[] = %v", got)
	}
}

func TestMaydayConnectorSyncPagesThroughSharedTimestamps(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.pageSize = 2
	tie, later := maydayTestTime, maydayTestTime.Add(time.Hour)
	for _, id := range []string{"t1", "t2", "t3", "t4", "t5"} {
		srv.contents["fr-fr"] = append(srv.contents["fr-fr"], maydayTestContent(id, id, "<p>"+id+"</p>", tie, true))
	}
	srv.contents["fr-fr"] = append(srv.contents["fr-fr"],
		maydayTestContent("l1", "l1", "<p>l1</p>", later, true),
		maydayTestContent("l2", "l2", "<p>l2</p>", later, true),
		maydayTestContent("l3", "l3", "<p>l3</p>", later.Add(time.Minute), true),
	)
	conn := srv.connector(t, nil)

	want := maydayIDs("fr-fr", "t1", "t2", "t3", "t4", "t5", "l1", "l2", "l3")
	if got := syncMaydayIDs(t, conn, SyncRequest{FromBeginning: true}); !slices.Equal(got, want) {
		t.Fatalf("source IDs = %v, want %v", got, want)
	}
	if got := pruneMaydayIDs(t, conn); !slices.Equal(got, want) {
		t.Fatalf("prune snapshot = %v, want %v", got, want)
	}
}

// TestMaydayConnectorWalkSurvivesConcurrentChanges deletes one article,
// creates another, and edits a third while a walk is between pages. With page
// offsets the delete shifts the listing so that c3 is never read, and the
// prune would remove it although it still exists.
func TestMaydayConnectorWalkSurvivesConcurrentChanges(t *testing.T) {
	for _, mode := range []string{"sync", "prune"} {
		t.Run(mode, func(t *testing.T) {
			srv := newMaydayFakeServer(t)
			srv.pageSize = 2
			srv.contents["fr-fr"] = maydayArticles("c", 4)
			changed := false
			srv.onContents = func(query url.Values) {
				if changed || query.Get("limit") == "1" {
					return
				}
				changed = true
				items := slices.DeleteFunc(srv.contents["fr-fr"], func(item map[string]any) bool { return item["id"] == "c1" })
				items = append(items, maydayTestContent("c5", "c5", "<p>c5</p>", maydayTestTime.Add(time.Hour), true))
				items[0]["updatedAt"] = maydayTestTime.Add(2 * time.Hour).UnixMilli() // c2 is edited
				srv.contents["fr-fr"] = items
			}
			conn := srv.connector(t, nil)

			var got []string
			if mode == "sync" {
				got = syncMaydayIDs(t, conn, SyncRequest{FromBeginning: true})
			} else {
				got = pruneMaydayIDs(t, conn)
			}
			for _, id := range maydayIDs("fr-fr", "c2", "c3", "c4", "c5") {
				if !slices.Contains(got, id) {
					t.Fatalf("%s listed %v, missing existing article %s", mode, got, id)
				}
			}
		})
	}
}

func TestMaydayConnectorSyncResumesAfterCommittedBatch(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.pageSize = 2
	srv.contents["fr-fr"] = maydayArticles("c", 5)
	// c4 shares c3's timestamp: resuming at c3 must still reach c4.
	srv.contents["fr-fr"][3]["updatedAt"] = srv.contents["fr-fr"][2]["updatedAt"]
	conn := srv.connector(t, map[string]any{"batch_size": 3})
	ctx := context.Background()

	first, err := conn.OpenSync(ctx, SyncRequest{FromBeginning: true})
	if err != nil {
		t.Fatalf("OpenSync: %v", err)
	}
	batch, err := first.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if got := maydayDocIDs(batch.Documents); !slices.Equal(got, maydayIDs("fr-fr", "c1", "c2", "c3")) {
		t.Fatalf("first batch = %v", got)
	}
	var cursor maydaySyncCursor
	if err := json.Unmarshal([]byte(batch.Checkpoint.Cursor), &cursor); err != nil {
		t.Fatalf("checkpoint cursor: %v", err)
	}
	if cursor.Locale != "fr-fr" || cursor.ContentID != "c3" || cursor.UpdatedAt != maydayTestTime.Add(3*time.Minute).UnixMilli() || cursor.Scopes == "" {
		t.Fatalf("checkpoint cursor = %+v", cursor)
	}

	got := syncMaydayIDs(t, conn, SyncRequest{FromBeginning: true, Resume: batch.Checkpoint})
	if !slices.Equal(got, maydayIDs("fr-fr", "c4", "c5")) {
		t.Fatalf("resumed documents = %v", got)
	}
}

func TestMaydayConnectorSyncResumesInLaterLocale(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = maydayArticles("f", 2)
	srv.contents["de-de"] = maydayArticles("d", 2)
	conn := srv.connector(t, map[string]any{"languages": "fr-fr, de-de", "batch_size": 3})
	ctx := context.Background()

	first, err := conn.OpenSync(ctx, SyncRequest{FromBeginning: true})
	if err != nil {
		t.Fatalf("OpenSync: %v", err)
	}
	batch, err := first.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	want := append(maydayIDs("fr-fr", "f1", "f2"), maydayIDs("de-de", "d1")...)
	if got := maydayDocIDs(batch.Documents); !slices.Equal(got, want) {
		t.Fatalf("first batch = %v, want %v", got, want)
	}
	if got := syncMaydayIDs(t, conn, SyncRequest{FromBeginning: true, Resume: batch.Checkpoint}); !slices.Equal(got, maydayIDs("de-de", "d2")) {
		t.Fatalf("resumed documents = %v", got)
	}
}

func TestMaydayConnectorSyncRejectsInvalidResume(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = maydayArticles("c", 2)
	srv.contents["de-de"] = maydayArticles("d", 1)
	conn := srv.connector(t, map[string]any{"batch_size": 1})
	ctx := context.Background()

	first, err := conn.OpenSync(ctx, SyncRequest{FromBeginning: true})
	if err != nil {
		t.Fatalf("OpenSync: %v", err)
	}
	batch, err := first.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	// Adding a language changes the scopes the checkpoint was taken under.
	widened := srv.connector(t, map[string]any{"languages": "fr-fr, de-de"})
	if _, err := widened.OpenSync(ctx, SyncRequest{Resume: batch.Checkpoint}); !errors.Is(err, ErrSyncResumeInvalid) {
		t.Fatalf("changed scopes error = %v, want ErrSyncResumeInvalid", err)
	}

	for _, checkpoint := range []*SyncCheckpoint{
		{},
		{Cursor: "not json"},
		{Cursor: `{"scopes":"x","locale":"fr-fr","updated_at":1}`},
		{Cursor: `{"scopes":"x","locale":"fr-fr","updated_at":1,"content_id":"c1"}`},
	} {
		if _, err := conn.OpenSync(ctx, SyncRequest{Resume: checkpoint}); !errors.Is(err, ErrSyncResumeInvalid) {
			t.Fatalf("checkpoint %+v error = %v, want ErrSyncResumeInvalid", checkpoint, err)
		}
	}
}

func TestMaydayConnectorPruneListsSyncedScope(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.pageSize = 2
	srv.collections["fr-fr"] = []map[string]any{{"id": maydayBillingCollectionID, "label": "Billing"}}
	srv.contents["fr-fr"] = []map[string]any{
		maydayTestContent("c1", "c1", "<p>c1</p>", maydayTestTime, true, maydayBillingCollectionID),
		maydayTestContent("c2", "c2", "<p>c2</p>", maydayTestTime.Add(time.Minute), false, maydayBillingCollectionID),
		maydayTestContent("c3", "c3", "<p>c3</p>", maydayTestTime.Add(2*time.Minute), true, maydayBillingCollectionID),
		maydayTestContent("c4", "c4", "<p>c4</p>", maydayTestTime.Add(3*time.Minute), true, maydayBillingCollectionID),
		maydayTestContent("other", "o", "<p>o</p>", maydayTestTime.Add(4*time.Minute), true, maydaySupportCollectionID),
	}

	if got, want := pruneMaydayIDs(t, srv.connector(t, map[string]any{"collections": "Billing"})), maydayIDs("fr-fr", "c1", "c3", "c4"); !slices.Equal(got, want) {
		t.Fatalf("prune snapshot = %v, want %v", got, want)
	}
}

func TestMaydayConnectorPruneSkippedWhenNothingListed(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = []map[string]any{maydayTestContent("c1", "c1", "<p>c1</p>", maydayTestTime, false)}
	if _, err := srv.connector(t, nil).OpenPrune(context.Background(), PruneRequest{}); !errors.Is(err, ErrPruneUnsupported) {
		t.Fatalf("OpenPrune error = %v, want ErrPruneUnsupported", err)
	}
}

func TestMaydayConnectorDetectsLocalesWhenNoneConfigured(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["de-de"] = []map[string]any{maydayTestContent("d1", "Rechnungen", "<p>Rechnungen</p>", maydayTestTime, true)}
	srv.contents["fr-fr"] = []map[string]any{
		maydayTestContent("f1", "Factures", "<p>Factures</p>", maydayTestTime, true),
		maydayTestContent("f2", "Brouillon", "<p>Brouillon</p>", maydayTestTime, false),
	}
	srv.contents["en-gb"] = []map[string]any{maydayTestContent("e1", "Draft", "<p>Draft</p>", maydayTestTime, false)}
	conn := srv.connector(t, map[string]any{"languages": ""})

	// en-gb holds only a draft, so it is not detected; detected locales keep
	// the fixed maydayLocales order.
	want := append(maydayIDs("fr-fr", "f1"), maydayIDs("de-de", "d1")...)
	if got := syncMaydayIDs(t, conn, SyncRequest{FromBeginning: true}); !slices.Equal(got, want) {
		t.Fatalf("source IDs = %v, want %v", got, want)
	}
	if got := srv.probeCount(); got != len(maydayLocales) {
		t.Fatalf("locale probes = %d, want one per Mayday locale (%d)", got, len(maydayLocales))
	}
	if got := pruneMaydayIDs(t, conn); !slices.Equal(got, want) {
		t.Fatalf("prune snapshot = %v, want %v", got, want)
	}
}

func TestMaydayConnectorDetectedLocalesResolveCollectionNamesPerLocale(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.collections["fr-fr"] = []map[string]any{{"id": maydaySupportCollectionID, "label": "Assistance"}}
	srv.collections["de-de"] = []map[string]any{{"id": maydayBillingCollectionID, "label": "Billing"}}
	srv.contents["fr-fr"] = []map[string]any{maydayTestContent("f1", "Aide", "<p>Aide</p>", maydayTestTime, true, maydaySupportCollectionID)}
	srv.contents["de-de"] = []map[string]any{
		maydayTestContent("d1", "Rechnungen", "<p>Rechnungen</p>", maydayTestTime, true, maydayBillingCollectionID),
		maydayTestContent("d2", "Hilfe", "<p>Hilfe</p>", maydayTestTime, true, maydaySupportCollectionID),
	}

	// "Billing" exists only in de-de, so fr-fr is skipped rather than synced
	// without a collection filter.
	got := syncMaydayIDs(t, srv.connector(t, map[string]any{"languages": "", "collections": "Billing"}), SyncRequest{FromBeginning: true})
	if !slices.Equal(got, maydayIDs("de-de", "d1")) {
		t.Fatalf("source IDs = %v", got)
	}

	_, err := srv.connector(t, map[string]any{"languages": "", "collections": "Unknown"}).OpenSync(context.Background(), SyncRequest{FromBeginning: true})
	var valErr *ConnectorValidationError
	if !errors.As(err, &valErr) || !strings.Contains(err.Error(), "any language") {
		t.Fatalf("unknown collection error = %T %v", err, err)
	}
}

func TestMaydayConnectorDetectionFailures(t *testing.T) {
	srv := newMaydayFakeServer(t)
	ctx := context.Background()

	var valErr *ConnectorValidationError
	err := srv.connector(t, map[string]any{"languages": ""}).ValidateConnectorSetting(ctx, nil)
	if !errors.As(err, &valErr) || !strings.Contains(err.Error(), "no published articles in any language") {
		t.Fatalf("empty account error = %T %v", err, err)
	}

	srv.set(func(s *maydayFakeServer) { s.forbidContents = true })
	_, err = srv.connector(t, map[string]any{"languages": ""}).OpenPrune(ctx, PruneRequest{})
	var apiErr *maydayAPIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden || errors.Is(err, context.Canceled) {
		t.Fatalf("detection failure = %T %v, want the HTTP 403 rather than a cancellation", err, err)
	}
}

func TestMaydayDocumentTimestampsAndText(t *testing.T) {
	iso := maydayContent{ID: "a", UpdatedAtISO: "2026-03-01T12:00:00.250Z"}
	if got := maydayUpdatedAtMillis(iso); got != maydayTestTime.Add(250*time.Millisecond).UnixMilli() {
		t.Fatalf("ISO fallback = %d", got)
	}
	if got := maydayUpdatedAtMillis(maydayContent{ID: "a"}); got != 0 {
		t.Fatalf("missing timestamps = %d, want 0", got)
	}
	if _, ok := contentDocument("fr-fr", maydayContent{ID: "a", Body: "<p> </p>"}, false); ok {
		t.Fatalf("an article without text must be skipped")
	}
	doc, ok := contentDocument("fr-fr", maydayContent{ID: "a", Body: "<p>text</p>"}, false)
	if !ok || doc.SemanticIdentifier != "a" || !doc.UpdatedAt.IsZero() {
		t.Fatalf("untitled document = %+v, %v", doc, ok)
	}
}

func maydayTiedArticles(prefix string, count int, at time.Time) []map[string]any {
	items := make([]map[string]any, 0, count)
	for i := 1; i <= count; i++ {
		id := fmt.Sprintf("%s%d", prefix, i)
		items = append(items, maydayTestContent(id, id, "<p>"+id+"</p>", at, true))
	}
	return items
}

func maydayIsTieQuery(query url.Values) bool {
	return query.Get("updatedAt[gte]") != "" && query.Get("updatedAt[gte]") == query.Get("updatedAt[lte]")
}

// TestMaydayConnectorTieReadSurvivesChanges edits one of five articles sharing
// an updatedAt while they are read by offset, which shifts the later offsets.
// The changed totals make the pass repeat instead of skipping an article.
func TestMaydayConnectorTieReadSurvivesChanges(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.pageSize = 2
	srv.contents["fr-fr"] = maydayTiedArticles("t", 5, maydayTestTime)
	edited := false
	srv.onContents = func(query url.Values) {
		if edited || !maydayIsTieQuery(query) {
			return
		}
		edited = true
		srv.contents["fr-fr"][0]["updatedAt"] = maydayTestTime.Add(time.Hour).UnixMilli()
	}

	got := pruneMaydayIDs(t, srv.connector(t, nil))
	for _, id := range maydayIDs("fr-fr", "t1", "t2", "t3", "t4", "t5") {
		if !slices.Contains(got, id) {
			t.Fatalf("prune listed %v, missing existing article %s", got, id)
		}
	}
}

// TestMaydayConnectorTieReadNeverTruncates serves tied articles in a different
// order on every request, as an unstable server sort could. Collecting IDs
// across passes must still list every article.
func TestMaydayConnectorTieReadNeverTruncates(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.pageSize = 2
	srv.rotateTies = 1
	srv.contents["fr-fr"] = maydayTiedArticles("t", 5, maydayTestTime)

	session, err := srv.connector(t, nil).OpenPrune(context.Background(), PruneRequest{})
	if err != nil {
		t.Fatalf("OpenPrune: %v", err)
	}
	var got []string
	for {
		batch, err := session.NextBatch(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("prune NextBatch with an unstable tie order: %v", err)
		}
		for _, doc := range batch.Documents {
			got = append(got, doc.SourceID)
		}
	}
	for _, id := range maydayIDs("fr-fr", "t1", "t2", "t3", "t4", "t5") {
		if !slices.Contains(got, id) {
			t.Fatalf("prune completed with %v, missing existing article %s", got, id)
		}
	}
}

func TestMaydayConnectorResumeInsideTiedArticles(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.pageSize = 2
	srv.contents["fr-fr"] = append(maydayTiedArticles("t", 5, maydayTestTime),
		maydayTestContent("later", "later", "<p>later</p>", maydayTestTime.Add(time.Hour), true))
	conn := srv.connector(t, map[string]any{"batch_size": 3})
	ctx := context.Background()

	first, err := conn.OpenSync(ctx, SyncRequest{FromBeginning: true})
	if err != nil {
		t.Fatalf("OpenSync: %v", err)
	}
	batch, err := first.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	anchor := batch.Documents[len(batch.Documents)-1].SourceID
	got := syncMaydayIDs(t, conn, SyncRequest{FromBeginning: true, Resume: batch.Checkpoint})
	if slices.Contains(got, anchor) {
		t.Fatalf("resume re-emitted the committed anchor %s", anchor)
	}
	for _, id := range maydayIDs("fr-fr", "t1", "t2", "t3", "t4", "t5", "later") {
		if !slices.Contains(got, id) && !slices.Contains(maydayDocIDs(batch.Documents), id) {
			t.Fatalf("resume missed %s: first batch %v, resumed %v", id, maydayDocIDs(batch.Documents), got)
		}
	}
}

func TestMaydayConnectorUndisclosedPageCap(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.pageSize = 2
	srv.hideCap = true
	srv.contents["fr-fr"] = maydayArticles("c", 5)

	if got, want := syncMaydayIDs(t, srv.connector(t, nil), SyncRequest{FromBeginning: true}), maydayIDs("fr-fr", "c1", "c2", "c3", "c4", "c5"); !slices.Equal(got, want) {
		t.Fatalf("source IDs = %v, want %v", got, want)
	}
}

func TestMaydayConnectorRejectsUnsortedListing(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.reverseOrder = true
	srv.contents["fr-fr"] = maydayArticles("c", 3)

	session, err := srv.connector(t, nil).OpenPrune(context.Background(), PruneRequest{})
	if err != nil {
		t.Fatalf("OpenPrune: %v", err)
	}
	if _, err := session.NextBatch(context.Background()); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("NextBatch error = %v, want an out-of-order failure", err)
	}
}

// TestMaydayConnectorPruneRunsWhenOneScopeIsEmpty keeps pruning when only one
// language is empty: skipping would leave deleted articles indexed forever.
func TestMaydayConnectorPruneRunsWhenOneScopeIsEmpty(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = maydayArticles("c", 2)
	got := pruneMaydayIDs(t, srv.connector(t, map[string]any{"languages": "fr-fr, en-gb"}))
	if want := maydayIDs("fr-fr", "c1", "c2"); !slices.Equal(got, want) {
		t.Fatalf("prune snapshot = %v, want %v", got, want)
	}
}

func TestMaydayConnectorSyncWindowWithSubMillisecondBounds(t *testing.T) {
	srv := newMaydayFakeServer(t)
	start := maydayTestTime.Add(500 * time.Microsecond)
	end := maydayTestTime.Add(time.Hour + 700*time.Microsecond)
	srv.contents["fr-fr"] = []map[string]any{
		maydayTestContent("before-start", "a", "<p>a</p>", maydayTestTime, true),
		maydayTestContent("after-start", "b", "<p>b</p>", maydayTestTime.Add(time.Millisecond), true),
		maydayTestContent("at-end", "c", "<p>c</p>", maydayTestTime.Add(time.Hour), true),
		maydayTestContent("after-end", "d", "<p>d</p>", maydayTestTime.Add(time.Hour+time.Millisecond), true),
	}

	got := syncMaydayIDs(t, srv.connector(t, nil), SyncRequest{WindowStart: &start, WindowEnd: end})
	if want := maydayIDs("fr-fr", "after-start", "at-end"); !slices.Equal(got, want) {
		t.Fatalf("source IDs = %v, want %v", got, want)
	}
}

func TestMaydayRetryStatusHonoursRetryAfter(t *testing.T) {
	response := func(retryAfter string) *http.Response {
		resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{}}
		resp.Header.Set("Retry-After", retryAfter)
		return resp
	}
	if wait, retry := maydayRetryStatus(response("2"), 1); !retry || wait != 2*time.Second {
		t.Fatalf("seconds: wait=%v retry=%v", wait, retry)
	}
	if wait, retry := maydayRetryStatus(response("99999999999"), 1); !retry || wait != maydayMaxRetryWait {
		t.Fatalf("huge seconds: wait=%v retry=%v, want the cap", wait, retry)
	}
	date := time.Now().Add(5 * time.Second).UTC().Format(http.TimeFormat)
	if wait, retry := maydayRetryStatus(response(date), 1); !retry || wait <= 0 || wait > 6*time.Second {
		t.Fatalf("HTTP date: wait=%v retry=%v", wait, retry)
	}
	if _, retry := maydayRetryStatus(response("1"), maydayMaxRateLimitRetries+1); retry {
		t.Fatalf("retried past maydayMaxRetries")
	}
	if _, retry := maydayRetryStatus(&http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{}}, 1); retry {
		t.Fatalf("retried a 400")
	}
}

// TestMaydayConnectorSurvivesTotalAboveListing reports one item more than the
// listing holds, as when an article is deleted between Mayday counting and
// listing. The walk used to index an empty page and crash the syncer.
func TestMaydayConnectorSurvivesTotalAboveListing(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.pageSize = 2
	srv.inflateTotal = 1
	srv.contents["fr-fr"] = maydayArticles("c", 3)
	conn := srv.connector(t, nil)

	want := maydayIDs("fr-fr", "c1", "c2", "c3")
	if got := syncMaydayIDs(t, conn, SyncRequest{FromBeginning: true}); !slices.Equal(got, want) {
		t.Fatalf("source IDs = %v, want %v", got, want)
	}
	if got := pruneMaydayIDs(t, conn); !slices.Equal(got, want) {
		t.Fatalf("prune snapshot = %v, want %v", got, want)
	}
}

func TestMaydayConnectorHalvesOversizedPages(t *testing.T) {
	srv := newMaydayFakeServer(t)
	body := "<p>" + strings.Repeat("x", 600) + "</p>"
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("c%d", i)
		srv.contents["fr-fr"] = append(srv.contents["fr-fr"], maydayTestContent(id, id, body, maydayTestTime.Add(time.Duration(i)*time.Minute), true))
	}
	origMax := maydayMaxResponseBytes
	maydayMaxResponseBytes = 2000
	t.Cleanup(func() { maydayMaxResponseBytes = origMax })

	got := syncMaydayIDs(t, srv.connector(t, nil), SyncRequest{FromBeginning: true})
	if want := maydayIDs("fr-fr", "c1", "c2", "c3", "c4"); !slices.Equal(got, want) {
		t.Fatalf("source IDs = %v, want %v", got, want)
	}
	queries := srv.listingRequests()
	if limit := queries[len(queries)-1].Get("limit"); limit == "100" {
		t.Fatalf("page size was never reduced: last limit %s", limit)
	}
}

func TestMaydayConnectorSkipsLocalesAnsweredWithBadRequest(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.badRequest = map[string]bool{"fr-fr": true, "en-gb": true}
	srv.contents["de-de"] = maydayArticles("d", 1)
	conn := srv.connector(t, map[string]any{"languages": ""})

	if err := conn.ValidateConnectorSetting(context.Background(), nil); err != nil {
		t.Fatalf("ValidateConnectorSetting: %v", err)
	}
	if got := syncMaydayIDs(t, conn, SyncRequest{FromBeginning: true}); !slices.Equal(got, maydayIDs("de-de", "d1")) {
		t.Fatalf("source IDs = %v", got)
	}
}

func TestMaydayConnectorFallsBackToDefaultLocale(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents[maydayFallbackLocale] = maydayArticles("c", 2)
	conn := srv.connector(t, map[string]any{"languages": ""})

	want := maydayIDs(maydayFallbackLocale, "c1", "c2")
	if got := syncMaydayIDs(t, conn, SyncRequest{FromBeginning: true}); !slices.Equal(got, want) {
		t.Fatalf("source IDs = %v, want %v", got, want)
	}
	if got := pruneMaydayIDs(t, conn); !slices.Equal(got, want) {
		t.Fatalf("prune snapshot = %v, want %v", got, want)
	}
}

func TestMaydayConnectorRetriesTokenRequest(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.tokenFailures = []int{http.StatusServiceUnavailable, http.StatusTooManyRequests}
	if err := srv.connector(t, nil).Validate(context.Background()); err != nil {
		t.Fatalf("Validate after transient token failures: %v", err)
	}
}

func TestMaydayConnectorFullSyncStopsAtWindowEnd(t *testing.T) {
	srv := newMaydayFakeServer(t)
	end := maydayTestTime.Add(time.Hour)
	srv.contents["fr-fr"] = []map[string]any{
		maydayTestContent("before", "a", "<p>a</p>", maydayTestTime, true),
		maydayTestContent("after", "b", "<p>b</p>", end.Add(time.Minute), true),
	}

	got := syncMaydayIDs(t, srv.connector(t, nil), SyncRequest{FromBeginning: true, WindowEnd: end})
	if !slices.Equal(got, maydayIDs("fr-fr", "before")) {
		t.Fatalf("source IDs = %v; an article edited after the window end belongs to the next sync", got)
	}
	if query := srv.listingRequests()[0]; query.Get("updatedAt[lte]") != "2026-03-01T13:00:00.000Z" || query.Get("updatedAt[gt]") != "" {
		t.Fatalf("full sync filters = %v", query)
	}
}

func TestMaydayConnectorDocumentsCarryEveryMetadataKey(t *testing.T) {
	srv := newMaydayFakeServer(t)
	item := maydayTestContent("c1", "c1", "<p>c1</p>", maydayTestTime, true)
	delete(item, "selfcareId")
	srv.contents["fr-fr"] = []map[string]any{item}

	session, err := srv.connector(t, nil).OpenSync(context.Background(), SyncRequest{FromBeginning: true})
	if err != nil {
		t.Fatalf("OpenSync: %v", err)
	}
	doc := drainMaydaySync(t, session)[0]
	for _, key := range []string{"content_id", "locale", "content_type", "knowledge_id", "selfcare_id", "collection_ids", "is_outdated"} {
		if _, ok := doc.Metadata[key]; !ok {
			t.Fatalf("metadata %v lacks %q", doc.Metadata, key)
		}
	}
	if ids, ok := doc.Metadata["collection_ids"].([]string); !ok || ids == nil || len(ids) != 0 {
		t.Fatalf("collection_ids = %#v, want an empty list", doc.Metadata["collection_ids"])
	}
	if doc.SemanticIdentifier != "c1" {
		t.Fatalf("single-language name = %q, want no locale suffix", doc.SemanticIdentifier)
	}
}

func TestMaydayConnectorPruneDropsArticlesWithoutText(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = []map[string]any{
		maydayTestContent("c1", "c1", "<p>c1</p>", maydayTestTime, true),
		maydayTestContent("c2", "c2", "<p> </p>", maydayTestTime.Add(time.Minute), true),
	}
	if got := pruneMaydayIDs(t, srv.connector(t, nil)); !slices.Equal(got, maydayIDs("fr-fr", "c1")) {
		t.Fatalf("prune snapshot = %v; an article without text must not be retained", got)
	}
}

// TestMaydayConnectorPruneFailsOnVanishingListing empties Mayday after
// OpenPrune found articles. The snapshot must fail rather than prune the
// whole dataset.
func TestMaydayConnectorPruneFailsOnVanishingListing(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = maydayArticles("c", 2)
	srv.onContents = func(query url.Values) {
		if query.Get("limit") == "1" {
			srv.contents["fr-fr"] = nil
		}
	}
	session, err := srv.connector(t, nil).OpenPrune(context.Background(), PruneRequest{})
	if err != nil {
		t.Fatalf("OpenPrune: %v", err)
	}
	if _, err := session.NextBatch(context.Background()); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("NextBatch error = %v, want a failed snapshot", err)
	}
}

func TestMaydayConnectorCollectionListKeepsCommas(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.collections["fr-fr"] = []map[string]any{{"id": maydayBillingCollectionID, "label": "Billing, Payments"}}
	srv.contents["fr-fr"] = []map[string]any{maydayTestContent("c1", "c1", "<p>c1</p>", maydayTestTime, true, maydayBillingCollectionID)}

	got := syncMaydayIDs(t, srv.connector(t, map[string]any{"collections": []any{"Billing, Payments"}}), SyncRequest{FromBeginning: true})
	if !slices.Equal(got, maydayIDs("fr-fr", "c1")) {
		t.Fatalf("source IDs = %v", got)
	}
}

func TestMaydayConnectorDetectionReportsAForbiddenLocale(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = maydayArticles("f", 1)
	srv.forbidLocale = map[string]bool{"de-de": true}

	_, err := srv.connector(t, map[string]any{"languages": ""}).OpenSync(context.Background(), SyncRequest{FromBeginning: true})
	if maydayStatus(err) != http.StatusForbidden {
		t.Fatalf("OpenSync error = %v, want the HTTP 403 rather than skipping de-de", err)
	}
}

// TestMaydayConnectorPruneDetectsLocalesTwice answers the first de-de probe
// empty, as a transient Mayday hiccup could. The second detection still finds
// de-de, so the prune keeps its documents.
func TestMaydayConnectorPruneDetectsLocalesTwice(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = maydayArticles("f", 1)
	srv.contents["de-de"] = maydayArticles("d", 1)
	srv.emptyProbes = map[string]int{"de-de": 1}

	got := pruneMaydayIDs(t, srv.connector(t, map[string]any{"languages": ""}))
	if want := append(maydayIDs("fr-fr", "f1"), maydayIDs("de-de", "d1")...); !slices.Equal(got, want) {
		t.Fatalf("prune snapshot = %v, want %v", got, want)
	}
}

// TestMaydayConnectorPruneRewalksAnEmptyScope empties fr-fr for exactly one
// listing. The prune walks the empty scope again instead of dropping fr-fr.
func TestMaydayConnectorPruneRewalksAnEmptyScope(t *testing.T) {
	srv := newMaydayFakeServer(t)
	srv.contents["fr-fr"] = maydayArticles("f", 2)
	srv.contents["en-gb"] = maydayArticles("e", 1)
	var stashed []map[string]any
	srv.onContents = func(query url.Values) {
		if query.Get("locale") != "fr-fr" {
			return
		}
		switch {
		case query.Get("limit") == "1" && stashed == nil:
			stashed, srv.contents["fr-fr"] = srv.contents["fr-fr"], nil
		case query.Get("limit") != "1" && stashed != nil:
			srv.contents["fr-fr"], stashed = stashed, nil
		}
	}

	got := pruneMaydayIDs(t, srv.connector(t, map[string]any{"languages": "fr-fr, en-gb"}))
	if want := append(maydayIDs("fr-fr", "f1", "f2"), maydayIDs("en-gb", "e1")...); !slices.Equal(got, want) {
		t.Fatalf("prune snapshot = %v, want %v", got, want)
	}
}

func TestMaydayConnectorTieReadIsBounded(t *testing.T) {
	cases := []struct {
		name    string
		inflate int
		want    string
	}{
		{"total never reached", 1, "kept changing"},
		{"total above the cap", maydayMaxTieTotal, "same instant"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newMaydayFakeServer(t)
			srv.pageSize = 2
			srv.inflateTotal = tc.inflate
			srv.contents["fr-fr"] = maydayTiedArticles("t", 4, maydayTestTime)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			session, err := srv.connector(t, nil).OpenSync(ctx, SyncRequest{FromBeginning: true})
			if err != nil {
				t.Fatalf("OpenSync: %v", err)
			}
			for {
				_, err = session.NextBatch(ctx)
				if err != nil {
					break
				}
			}
			if errors.Is(err, io.EOF) || errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("walk error = %v, want one mentioning %q", err, tc.want)
			}
		})
	}
}

func TestMaydayConnectorRejectsInvalidContentIDs(t *testing.T) {
	for name, id := range map[string]string{"empty": "", "oversized": strings.Repeat("a", maydayMaxIDBytes+1)} {
		t.Run(name, func(t *testing.T) {
			srv := newMaydayFakeServer(t)
			srv.contents["fr-fr"] = []map[string]any{maydayTestContent(id, "a", "<p>a</p>", maydayTestTime, true)}
			session, err := srv.connector(t, nil).OpenSync(context.Background(), SyncRequest{FromBeginning: true})
			if err != nil {
				t.Fatalf("OpenSync: %v", err)
			}
			if _, err := session.NextBatch(context.Background()); err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("NextBatch error = %v, want a rejected content id", err)
			}
		})
	}
}

func TestMaydayRetryBudgets(t *testing.T) {
	status := func(code int) *http.Response { return &http.Response{StatusCode: code, Header: http.Header{}} }
	if wait, retry := maydayRetryStatus(status(http.StatusTooManyRequests), 1); !retry || wait != 5*maydayRetryBaseDelay {
		t.Fatalf("first 429: wait=%v retry=%v, want %v", wait, retry, 5*maydayRetryBaseDelay)
	}
	if _, retry := maydayRetryStatus(status(http.StatusTooManyRequests), maydayMaxRetries+1); !retry {
		t.Fatalf("a 429 must outlast the server-error retry budget")
	}
	if _, retry := maydayRetryStatus(status(http.StatusServiceUnavailable), maydayMaxRetries+1); retry {
		t.Fatalf("a 503 retried past maydayMaxRetries")
	}

	srv := newMaydayFakeServer(t)
	srv.contentFailures = slices.Repeat([]int{http.StatusTooManyRequests}, maydayMaxRateLimitRetries)
	if err := srv.connector(t, nil).Validate(context.Background()); err != nil {
		t.Fatalf("Validate after %d rate-limited attempts: %v", maydayMaxRateLimitRetries, err)
	}
	if got := srv.probeCount(); got != maydayMaxRateLimitRetries+1 {
		t.Fatalf("probe attempts = %d, want %d", got, maydayMaxRateLimitRetries+1)
	}
}

func TestMaydayConnectorMarksOutdatedArticles(t *testing.T) {
	srv := newMaydayFakeServer(t)
	outdated := maydayTestContent("old", "old", "<p>same text</p>", maydayTestTime, true)
	outdated["isOutdated"] = true
	current := maydayTestContent("new", "new", "<p>same text</p>", maydayTestTime.Add(time.Minute), true)
	srv.contents["fr-fr"] = []map[string]any{outdated, current}
	conn := srv.connector(t, nil)
	sync := func() map[string]SourceDocument {
		session, err := conn.OpenSync(context.Background(), SyncRequest{FromBeginning: true})
		if err != nil {
			t.Fatalf("OpenSync: %v", err)
		}
		docs := map[string]SourceDocument{}
		for _, doc := range drainMaydaySync(t, session) {
			docs[doc.Metadata["content_id"].(string)] = doc
		}
		return docs
	}

	docs := sync()
	if got := docs["old"].Metadata["is_outdated"]; got != "true" {
		t.Fatalf("outdated article is_outdated = %#v, want \"true\"", got)
	}
	if got := docs["new"].Metadata["is_outdated"]; got != "false" {
		t.Fatalf("current article is_outdated = %#v, want \"false\"", got)
	}

	// Marking an article outdated without touching its text must still
	// change the fingerprint, or the runner would skip the metadata update.
	before := docs["new"].Fingerprint
	srv.set(func(s *maydayFakeServer) { s.contents["fr-fr"][1]["isOutdated"] = true })
	after := sync()["new"]
	if after.Metadata["is_outdated"] != "true" || after.Fingerprint == before {
		t.Fatalf("after marking outdated: is_outdated=%v, fingerprint changed=%v", after.Metadata["is_outdated"], after.Fingerprint != before)
	}
}

func TestSyncSourceFilenameKeepsUTF8Valid(t *testing.T) {
	title := strings.Repeat("é", 200) // 400 bytes, cut inside a character
	name := syncSourceDocumentFilenameFromDocument(SourceDocument{SemanticIdentifier: title, Extension: ".md"})
	if !utf8.ValidString(name) || len(name) > 255 || !strings.HasSuffix(name, ".md") {
		t.Fatalf("filename = %q (%d bytes, valid UTF-8: %v)", name, len(name), utf8.ValidString(name))
	}
}

func TestMaydayErrorBodyIsTruncated(t *testing.T) {
	long := strings.Repeat("é", maydayMaxErrorBytes)
	got := maydayErrorBody([]byte(long))
	if len(got) > maydayMaxErrorBytes+len("...") || !strings.HasSuffix(got, "...") || !strings.HasPrefix(got, "é") {
		t.Fatalf("truncated body = %d bytes", len(got))
	}
}
