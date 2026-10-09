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
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultMaydayAPIBaseURL  = "https://api.mayday.fr/v1"
	defaultMaydayOAuthURL    = "https://api.mayday.fr/oauth/token"
	defaultMaydayBatchSize   = 30
	maydayMaxPageSize        = 30
	maydayPrunePageSize      = 100
	maydayRequestTimeout     = 60 * time.Second
	maydaySourceIDPrefix     = "mayday_article_"
)

// maydayToken stores an acquired OAuth 2.0 access token with its expiry time.
type maydayToken struct {
	AccessToken string
	ExpiresAt   time.Time
}

// MaydayConnector reads Mayday knowledge base articles and collections.
// It authenticates via the OAuth 2.0 Client Credentials grant.
type MaydayConnector struct {
	clientID      string
	clientSecret  string
	baseURL       string
	tokenURL      string
	batchSize     int
	collections   map[string]struct{}
	languages     map[string]struct{}
	includeDrafts bool
	httpClient    *http.Client

	tokenMu     sync.Mutex
	cachedToken *maydayToken
	now         func() time.Time

	acquireToken func(ctx context.Context) (maydayToken, error)
	doJSON       func(ctx context.Context, endpoint string, query url.Values, out any) error
}

type maydayAPIError struct {
	Status  int
	Message string
}

func (e *maydayAPIError) Error() string {
	return fmt.Sprintf("Mayday API returned HTTP %d: %s", e.Status, e.Message)
}

// NewMaydayConnector creates a Mayday connector from config.
func NewMaydayConnector(config map[string]any) (*MaydayConnector, error) {
	credentials := configAnyMap(config["credentials"])
	clientID := strings.TrimSpace(firstNonEmpty(stringConfig(credentials["client_id"]), stringConfig(config["client_id"])))
	clientSecret := strings.TrimSpace(firstNonEmpty(stringConfig(credentials["client_secret"]), stringConfig(config["client_secret"])))

	baseURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(stringConfig(config["base_url"]), stringConfig(credentials["base_url"]), defaultMaydayAPIBaseURL)), "/")
	tokenURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(stringConfig(config["token_url"]), stringConfig(credentials["token_url"]), defaultMaydayOAuthURL)), "/")

	batchSize := maydayBatchSize(firstNonEmpty(stringConfig(config["sync_batch_size"]), stringConfig(config["batch_size"])))

	connector := &MaydayConnector{
		clientID:      clientID,
		clientSecret:  clientSecret,
		baseURL:       baseURL,
		tokenURL:      tokenURL,
		batchSize:     batchSize,
		collections:   parseStringSet(config["collections"]),
		languages:     parseStringSet(config["languages"]),
		includeDrafts: boolConfig(config["include_drafts"]),
		httpClient:    &http.Client{Timeout: maydayRequestTimeout},
		now:           time.Now,
	}
	connector.acquireToken = connector.requestAccessToken
	connector.doJSON = connector.executeMaydayJSON
	return connector, nil
}

func maydayBatchSize(value any) int {
	switch typed := value.(type) {
	case int:
		if typed > 0 {
			return typed
		}
	case int64:
		if typed > 0 {
			return int(typed)
		}
	case float64:
		if typed > 0 {
			return int(typed)
		}
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil && parsed > 0 {
			return parsed
		}
	}
	return defaultMaydayBatchSize
}

func parseStringSet(value any) map[string]struct{} {
	result := make(map[string]struct{})
	switch typed := value.(type) {
	case string:
		for _, item := range strings.Split(typed, ",") {
			trimmed := strings.ToLower(strings.TrimSpace(item))
			if trimmed != "" {
				result[trimmed] = struct{}{}
			}
		}
	case []string:
		for _, item := range typed {
			trimmed := strings.ToLower(strings.TrimSpace(item))
			if trimmed != "" {
				result[trimmed] = struct{}{}
			}
		}
	case []any:
		for _, item := range typed {
			trimmed := strings.ToLower(strings.TrimSpace(fmt.Sprint(item)))
			if trimmed != "" {
				result[trimmed] = struct{}{}
			}
		}
	}
	return result
}

func boolConfig(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		lower := strings.ToLower(strings.TrimSpace(typed))
		return lower == "true" || lower == "1" || lower == "yes"
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	}
	return false
}

// Validate validates Mayday settings and credentials against the configured API.
func (c *MaydayConnector) Validate(ctx context.Context) error {
	if c == nil {
		return &ConnectorValidationError{Message: "Mayday connector is nil"}
	}
	if c.clientID == "" || c.clientSecret == "" {
		return &ConnectorMissingCredentialError{Message: "Mayday credentials must include client_id and client_secret"}
	}
	if c.baseURL == "" {
		return &ConnectorValidationError{Message: "Mayday base_url must not be empty"}
	}
	if c.tokenURL == "" {
		return &ConnectorValidationError{Message: "Mayday token_url must not be empty"}
	}
	if _, _, err := assertConnectorURLSafe(c.baseURL); err != nil {
		return &ConnectorValidationError{Message: fmt.Sprintf("invalid Mayday base URL: %v", err)}
	}
	if _, _, err := assertConnectorURLSafe(c.tokenURL); err != nil {
		return &ConnectorValidationError{Message: fmt.Sprintf("invalid Mayday token URL: %v", err)}
	}

	// Verify authentication by acquiring a token
	if _, err := c.getOrRefreshToken(ctx); err != nil {
		return classifyMaydayError(err)
	}

	// Ping articles endpoint to ensure API read permissions
	query := url.Values{
		"page":  {"1"},
		"limit": {"1"},
	}
	var page maydayArticlePage
	if err := c.doJSON(ctx, "articles", query, &page); err != nil {
		return classifyMaydayError(err)
	}
	return nil
}

// ValidateConnectorSetting validates Mayday settings from an unsaved config.
func (c *MaydayConnector) ValidateConnectorSetting(ctx context.Context, request map[string]any) error {
	ctx, cancel := context.WithTimeout(ctx, connectorSettingValidationTimeout)
	defer cancel()
	return c.Validate(ctx)
}

func classifyMaydayError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr *maydayAPIError
	if errors.As(err, &apiErr) {
		switch apiErr.Status {
		case http.StatusUnauthorized:
			return &ConnectorMissingCredentialError{Message: "Mayday authorization failed. Please verify your client_id and client_secret."}
		case http.StatusForbidden:
			return &ConnectorValidationError{Message: "Mayday credentials lack permissions to access knowledge base articles."}
		case http.StatusNotFound:
			return &ConnectorValidationError{Message: "Mayday API endpoint not found. Please check your base_url."}
		case http.StatusTooManyRequests:
			return &ConnectorValidationError{Message: "Mayday rate limit exceeded during validation."}
		default:
			return &ConnectorValidationError{Message: fmt.Sprintf("Mayday API error (HTTP %d): %s", apiErr.Status, apiErr.Message)}
		}
	}
	var credErr *ConnectorMissingCredentialError
	if errors.As(err, &credErr) {
		return err
	}
	var valErr *ConnectorValidationError
	if errors.As(err, &valErr) {
		return err
	}
	return &ConnectorValidationError{Message: fmt.Sprintf("Mayday connection test failed: %v", err)}
}

// getOrRefreshToken retrieves the cached OAuth token or requests a new one.
func (c *MaydayConnector) getOrRefreshToken(ctx context.Context) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	now := c.now()
	if c.cachedToken != nil && now.Before(c.cachedToken.ExpiresAt.Add(-60*time.Second)) {
		return c.cachedToken.AccessToken, nil
	}

	token, err := c.acquireToken(ctx)
	if err != nil {
		return "", err
	}
	c.cachedToken = &token
	return token.AccessToken, nil
}

// invalidateCachedToken drops the current token on authorization failure.
func (c *MaydayConnector) invalidateCachedToken() {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	c.cachedToken = nil
}

// requestAccessToken performs the OAuth2 client_credentials grant.
func (c *MaydayConnector) requestAccessToken(ctx context.Context) (maydayToken, error) {
	if _, _, err := assertConnectorURLSafe(c.tokenURL); err != nil {
		return maydayToken{}, &ConnectorValidationError{Message: fmt.Sprintf("Mayday token URL unsafe: %v", err)}
	}

	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
	}

	requestCtx, cancel := context.WithTimeout(ctx, maydayRequestTimeout)
	defer cancel()

	resp, err := connectorRequest(requestCtx, connectorRequestOptions{
		Method:   http.MethodPost,
		RawURL:   c.tokenURL,
		Body:     []byte(form.Encode()),
		Headers:  map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
		Timeout:  maydayRequestTimeout,
		Validate: assertConnectorURLSafe,
	})
	if err != nil {
		return maydayToken{}, &ConnectorMissingCredentialError{Message: fmt.Sprintf("Mayday token request failed: %v", err)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
	if err != nil {
		return maydayToken{}, fmt.Errorf("read Mayday token response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return maydayToken{}, &maydayAPIError{Status: resp.StatusCode, Message: strings.TrimSpace(string(body))}
	}

	var payload struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
		Error       string `json:"error"`
		ErrorDesc   string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return maydayToken{}, fmt.Errorf("decode Mayday token JSON: %w", err)
	}
	if payload.AccessToken == "" {
		detail := firstNonEmpty(payload.ErrorDesc, payload.Error, "empty access_token received")
		return maydayToken{}, &ConnectorMissingCredentialError{Message: fmt.Sprintf("Mayday OAuth failed: %s", detail)}
	}

	expiresIn := payload.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}

	return maydayToken{
		AccessToken: payload.AccessToken,
		ExpiresAt:   c.now().Add(time.Duration(expiresIn) * time.Second),
	}, nil
}

// executeMaydayJSON executes an authenticated HTTP GET request against the Mayday API.
func (c *MaydayConnector) executeMaydayJSON(ctx context.Context, endpoint string, query url.Values, out any) error {
	token, err := c.getOrRefreshToken(ctx)
	if err != nil {
		return err
	}

	endpointURL := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	if len(query) > 0 {
		endpointURL += "?" + query.Encode()
	}

	sendReq := func(accessToken string) (*http.Response, []byte, error) {
		requestCtx, cancel := context.WithTimeout(ctx, maydayRequestTimeout)
		defer cancel()

		resp, reqErr := connectorRequest(requestCtx, connectorRequestOptions{
			Method: http.MethodGet,
			RawURL: endpointURL,
			Headers: map[string]string{
				"Authorization": "Bearer " + accessToken,
				"Accept":        "application/json",
			},
			Timeout:  maydayRequestTimeout,
			Validate: assertConnectorURLSafe,
		})
		if reqErr != nil {
			return nil, nil, reqErr
		}
		defer resp.Body.Close()

		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
		return resp, respBody, readErr
	}

	resp, body, err := sendReq(token)
	if err != nil {
		return err
	}

	// Retry once on 401 Unauthorized in case token was invalidated server-side
	if resp.StatusCode == http.StatusUnauthorized {
		c.invalidateCachedToken()
		freshToken, tokenErr := c.getOrRefreshToken(ctx)
		if tokenErr == nil {
			resp, body, err = sendReq(freshToken)
			if err != nil {
				return err
			}
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &maydayAPIError{Status: resp.StatusCode, Message: strings.TrimSpace(string(body))}
	}

	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("decode Mayday JSON from %s: %w", endpoint, err)
		}
	}
	return nil
}

// Mayday entity models
type maydayArticle struct {
	ID             string   `json:"id"`
	Title          string   `json:"title"`
	Content        string   `json:"content"`
	Body           string   `json:"body"`
	HTML           string   `json:"html"`
	CollectionID   string   `json:"collection_id"`
	CollectionName string   `json:"collection_name"`
	Language       string   `json:"language"`
	Tags           []string `json:"tags"`
	Status         string   `json:"status"`
	URL            string   `json:"url"`
	CreatedAt      string   `json:"created_at"`
	UpdatedAt      string   `json:"updated_at"`
}

type maydayArticlePage struct {
	Data       []maydayArticle `json:"data"`
	Articles   []maydayArticle `json:"articles"`
	Page       int             `json:"page"`
	TotalPages int             `json:"total_pages"`
	HasMore    bool            `json:"has_more"`
	NextCursor string          `json:"next_cursor"`
}

func (p *maydayArticlePage) items() []maydayArticle {
	if len(p.Data) > 0 {
		return p.Data
	}
	return p.Articles
}

type maydayCollection struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type maydayCollectionResponse struct {
	Data        []maydayCollection `json:"data"`
	Collections []maydayCollection `json:"collections"`
}

func (r *maydayCollectionResponse) items() []maydayCollection {
	if len(r.Data) > 0 {
		return r.Data
	}
	return r.Collections
}

// loadCollections retrieves and maps collection IDs to collection names.
func (c *MaydayConnector) loadCollections(ctx context.Context) (map[string]string, error) {
	var resp maydayCollectionResponse
	if err := c.doJSON(ctx, "collections", nil, &resp); err != nil {
		// Non-fatal if collections endpoint is unavailable; return empty map
		return map[string]string{}, nil
	}
	result := make(map[string]string)
	for _, col := range resp.items() {
		if col.ID != "" && col.Name != "" {
			result[col.ID] = col.Name
		}
	}
	return result, nil
}

func (c *MaydayConnector) listArticlesPage(ctx context.Context, page int, cursor string, limit int) (maydayArticlePage, error) {
	if limit <= 0 {
		limit = maydayMaxPageSize
	}
	query := url.Values{
		"limit": {strconv.Itoa(limit)},
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	} else if page > 0 {
		query.Set("page", strconv.Itoa(page))
	}

	var articlePage maydayArticlePage
	err := c.doJSON(ctx, "articles", query, &articlePage)
	return articlePage, err
}

func (c *MaydayConnector) isIndexableArticle(article maydayArticle, collectionName string) bool {
	// Status filtering: Skip drafts/archived unless explicitly included
	status := strings.ToLower(strings.TrimSpace(article.Status))
	if !c.includeDrafts && (status == "draft" || status == "archived") {
		return false
	}

	// Collection filtering
	if len(c.collections) > 0 {
		colID := strings.ToLower(strings.TrimSpace(article.CollectionID))
		colName := strings.ToLower(strings.TrimSpace(collectionName))
		_, idMatch := c.collections[colID]
		_, nameMatch := c.collections[colName]
		if !idMatch && !nameMatch {
			return false
		}
	}

	// Language filtering
	if len(c.languages) > 0 {
		lang := strings.ToLower(strings.TrimSpace(article.Language))
		if _, ok := c.languages[lang]; !ok {
			return false
		}
	}

	// Body check
	rawContent := firstNonEmpty(article.Content, article.Body, article.HTML)
	return strings.TrimSpace(rawContent) != ""
}

func parseMaydayTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999Z07:00",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	}
	for _, layout := range formats {
		if t, err := time.Parse(layout, raw); err == nil {
			return t
		}
	}
	return time.Time{}
}

func maydayArticleSourceID(id string) string {
	return maydaySourceIDPrefix + strings.TrimSpace(id)
}

func includeMaydayDocument(request SyncRequest, sourceID string, updatedAt time.Time, fingerprint string) bool {
	if request.FromBeginning {
		return true
	}
	if len(request.Fingerprints) > 0 {
		if fingerprint == "" {
			return true
		}
		stored, ok := request.Fingerprints[sourceID]
		return !ok || stored == "" || stored != fingerprint
	}
	if updatedAt.IsZero() {
		return true
	}
	return !beforeOrAtWindowStart(updatedAt, request.WindowStart) && !afterWindowEnd(updatedAt, request.WindowEnd)
}

// OpenSync opens a Mayday synchronization session.
func (c *MaydayConnector) OpenSync(ctx context.Context, request SyncRequest) (SyncSession, error) {
	if err := c.Validate(ctx); err != nil {
		return nil, err
	}

	collections, _ := c.loadCollections(ctx)

	session := &maydaySyncSession{
		connector:   c,
		request:     request,
		batchSize:   c.batchSize,
		collections: collections,
		currentPage: 1,
	}

	return session, nil
}

type maydaySyncCursor struct {
	Page       int       `json:"page"`
	NextCursor string    `json:"next_cursor"`
	SourceID   string    `json:"source_id"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type maydaySyncSession struct {
	connector   *MaydayConnector
	request     SyncRequest
	batchSize   int
	collections map[string]string
	done        bool

	currentPage int
	nextCursor  string
	hasMore     bool

	articleBuffer []maydayArticle
	articleIndex  int

	resumeSource  string
	resumeSkip    bool
	resumeChecked bool
}

func (s *maydaySyncSession) NextBatch(ctx context.Context) (SyncBatch, error) {
	if s.done {
		return SyncBatch{}, io.EOF
	}
	if err := s.validateResume(ctx); err != nil {
		return SyncBatch{}, err
	}

	documents := make([]SourceDocument, 0, s.batchSize)
	var last SourceDocument

	for len(documents) < s.batchSize && !s.done {
		doc, ok, err := s.nextArticle(ctx)
		if err != nil {
			return SyncBatch{}, err
		}
		if !ok {
			break
		}
		documents = append(documents, doc)
		last = doc
	}

	if len(documents) == 0 {
		return SyncBatch{}, io.EOF
	}
	return SyncBatch{Documents: documents, Checkpoint: s.checkpoint(last)}, nil
}

func (s *maydaySyncSession) Close() error {
	return nil
}

func (s *maydaySyncSession) validateResume(ctx context.Context) error {
	if s.resumeChecked || s.request.Resume == nil {
		return nil
	}
	s.resumeChecked = true
	checkpoint := s.request.Resume
	if checkpoint.Cursor == "" {
		return fmt.Errorf("mayday sync cursor is missing: %w", ErrSyncResumeInvalid)
	}

	var cursor maydaySyncCursor
	if err := json.Unmarshal([]byte(checkpoint.Cursor), &cursor); err != nil {
		return fmt.Errorf("mayday sync cursor is invalid: %w", ErrSyncResumeInvalid)
	}

	sourceID := firstNonEmpty(cursor.SourceID, checkpoint.SourceID)
	if sourceID == "" || !strings.HasPrefix(sourceID, maydaySourceIDPrefix) {
		return fmt.Errorf("mayday sync checkpoint has invalid source anchor: %w", ErrSyncResumeInvalid)
	}

	s.resumeSource = sourceID
	s.resumeSkip = true
	if cursor.Page > 0 {
		s.currentPage = cursor.Page
	}
	s.nextCursor = cursor.NextCursor
	return nil
}

func (s *maydaySyncSession) nextArticle(ctx context.Context) (SourceDocument, bool, error) {
	for {
		if err := s.fillArticleBuffer(ctx); err != nil {
			return SourceDocument{}, false, err
		}
		if s.articleIndex >= len(s.articleBuffer) {
			if !s.hasMore {
				s.done = true
				return SourceDocument{}, false, nil
			}
			continue
		}

		article := s.articleBuffer[s.articleIndex]
		s.articleIndex++
		sourceID := maydayArticleSourceID(article.ID)

		if s.resumeSkip && s.resumeSource != "" {
			if sourceID == s.resumeSource {
				s.resumeSource = ""
				s.resumeSkip = false
			}
			continue
		}

		colName := article.CollectionName
		if colName == "" {
			colName = s.collections[article.CollectionID]
		}

		if !s.connector.isIndexableArticle(article, colName) {
			continue
		}

		doc, ok, err := s.articleDocument(article, colName)
		if err != nil {
			return SourceDocument{}, false, err
		}
		if !ok || !includeMaydayDocument(s.request, doc.SourceID, doc.UpdatedAt, doc.Fingerprint) {
			continue
		}

		return doc, true, nil
	}
}

func (s *maydaySyncSession) fillArticleBuffer(ctx context.Context) error {
	for s.articleIndex >= len(s.articleBuffer) {
		if s.done {
			return nil
		}
		if s.currentPage > 1 && !s.hasMore && s.nextCursor == "" {
			return nil
		}

		page, err := s.connector.listArticlesPage(ctx, s.currentPage, s.nextCursor, s.batchSize)
		if err != nil {
			return err
		}

		items := page.items()
		s.articleBuffer = items
		s.articleIndex = 0

		// Handle pagination progression
		s.hasMore = page.HasMore || (page.TotalPages > 0 && s.currentPage < page.TotalPages) || len(items) == s.batchSize
		if page.NextCursor != "" && page.NextCursor != s.nextCursor {
			s.nextCursor = page.NextCursor
		} else {
			s.nextCursor = ""
			s.currentPage++
		}

		if len(items) == 0 {
			s.hasMore = false
			return nil
		}
	}
	return nil
}

func (s *maydaySyncSession) articleDocument(article maydayArticle, collectionName string) (SourceDocument, bool, error) {
	rawContent := firstNonEmpty(article.Content, article.Body, article.HTML)
	if strings.TrimSpace(rawContent) == "" {
		return SourceDocument{}, false, nil
	}

	// Convert HTML markup to markdown / plain text
	var textBody string
	if converter := newMarkdownConverter(); converter != nil {
		if mdText, err := converter.ConvertString(rawContent); err == nil && strings.TrimSpace(mdText) != "" {
			textBody = strings.TrimSpace(mdText)
		}
	}
	if textBody == "" {
		textBody = htmlToText(rawContent)
	}
	if textBody == "" {
		return SourceDocument{}, false, nil
	}

	blob := []byte(textBody)
	metadata := map[string]any{
		"article_id": article.ID,
		"status":     firstNonEmpty(article.Status, "published"),
	}
	if article.CollectionID != "" {
		metadata["collection_id"] = article.CollectionID
	}
	if collectionName != "" {
		metadata["collection_name"] = collectionName
	}
	if article.Language != "" {
		metadata["language"] = article.Language
	}
	if len(article.Tags) > 0 {
		metadata["tags"] = article.Tags
	}
	if article.URL != "" {
		metadata["url"] = article.URL
	}

	updatedAt := parseMaydayTime(firstNonEmpty(article.UpdatedAt, article.CreatedAt))
	if updatedAt.IsZero() {
		updatedAt = s.connector.now()
	}

	doc := SourceDocument{
		SourceID:           maydayArticleSourceID(article.ID),
		SemanticIdentifier: firstNonEmpty(article.Title, article.ID),
		Extension:          ".txt",
		Blob:               blob,
		UpdatedAt:          updatedAt,
		SizeBytes:          int64(len(blob)),
		Metadata:           metadata,
		Fingerprint:        contentFingerprint(blob),
	}
	return doc, true, nil
}

func (s *maydaySyncSession) checkpoint(last SourceDocument) *SyncCheckpoint {
	if last.SourceID == "" {
		return nil
	}
	cursor := maydaySyncCursor{
		Page:       s.currentPage,
		NextCursor: s.nextCursor,
		SourceID:   last.SourceID,
		UpdatedAt:  last.UpdatedAt,
	}
	data, err := json.Marshal(cursor)
	if err != nil {
		return nil
	}
	updatedAt := last.UpdatedAt
	return &SyncCheckpoint{
		Cursor:    string(data),
		SourceID:  last.SourceID,
		UpdatedAt: &updatedAt,
	}
}

// OpenPrune opens a complete prune snapshot session.
func (c *MaydayConnector) OpenPrune(ctx context.Context, request PruneRequest) (PruneSession, error) {
	if err := c.Validate(ctx); err != nil {
		return nil, err
	}
	return &maydayPruneSession{
		connector:   c,
		currentPage: 1,
	}, nil
}

type maydayPruneSession struct {
	connector *MaydayConnector
	done      bool

	currentPage int
	nextCursor  string
	hasMore     bool

	articleBuffer []maydayArticle
	articleIndex  int
}

func (s *maydayPruneSession) NextBatch(ctx context.Context) (PruneBatch, error) {
	if s.done {
		return PruneBatch{}, io.EOF
	}

	documents := make([]SlimDocument, 0, maydayPrunePageSize)
	for len(documents) < maydayPrunePageSize && !s.done {
		if err := s.fillArticlePage(ctx); err != nil {
			return PruneBatch{}, err
		}

		for s.articleIndex < len(s.articleBuffer) {
			article := s.articleBuffer[s.articleIndex]
			s.articleIndex++

			if article.ID != "" {
				documents = append(documents, SlimDocument{
					SourceID: maydayArticleSourceID(article.ID),
				})
				if len(documents) >= maydayPrunePageSize {
					break
				}
			}
		}

		if s.articleIndex >= len(s.articleBuffer) && !s.hasMore && s.nextCursor == "" {
			s.done = true
		}
	}

	if len(documents) == 0 {
		return PruneBatch{}, io.EOF
	}
	return PruneBatch{Documents: documents}, nil
}

func (s *maydayPruneSession) Close() error {
	return nil
}

func (s *maydayPruneSession) fillArticlePage(ctx context.Context) error {
	for s.articleIndex >= len(s.articleBuffer) {
		if s.done {
			return nil
		}
		if s.currentPage > 1 && !s.hasMore && s.nextCursor == "" {
			return nil
		}

		page, err := s.connector.listArticlesPage(ctx, s.currentPage, s.nextCursor, maydayPrunePageSize)
		if err != nil {
			return err
		}

		items := page.items()
		s.articleBuffer = items
		s.articleIndex = 0

		s.hasMore = page.HasMore || (page.TotalPages > 0 && s.currentPage < page.TotalPages) || len(items) == maydayPrunePageSize
		if page.NextCursor != "" && page.NextCursor != s.nextCursor {
			s.nextCursor = page.NextCursor
		} else {
			s.nextCursor = ""
			s.currentPage++
		}

		if len(items) == 0 {
			s.hasMore = false
			return nil
		}
	}
	return nil
}
