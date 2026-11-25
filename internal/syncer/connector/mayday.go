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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The Mayday Public API is documented at https://mayday-hq.gitbook.io/mayday-public-api.
const (
	defaultMaydayAPIBaseURL   = "https://public-api.mayday.fr"
	defaultMaydayTokenURL     = "https://public-api.mayday.fr/auth/oauth2/token"
	defaultMaydayBatchSize    = 30
	maydayPageSize            = 100
	maydayContentType         = "Article"
	maydayFallbackLocale      = "default"
	maydayDetectWorkers       = 11
	maydayMaxRetries          = 3
	maydayMaxRateLimitRetries = 5
	maydayMaxRetryWait        = 60 * time.Second
	maydayMaxTiePasses        = 5
	maydayMaxTieTotal         = 100_000
	maydayMaxCollectionPages  = 1_000
	maydayMaxIDBytes          = 128
	maydayMaxLabelBytes       = 4096
	maydayMaxErrorBytes       = 512
	maydayRequestTimeout      = 5 * time.Minute
	maydayTokenRefreshSkew    = 60 * time.Second
	maydayDefaultTokenTTL     = 5 * time.Minute
	maydayMaxTokenTTL         = 24 * time.Hour
	maydayTimeFilterLayout    = "2006-01-02T15:04:05.000Z07:00"
	maydaySourceIDPrefix      = "mayday_content_"
)

// Tests shorten these. maydayRetryBaseDelay is the first backoff step for
// retried requests; rate-limited requests back off five times longer, since
// Mayday limits requests per minute. maydayMaxResponseBytes caps one response.
var (
	maydayRetryBaseDelay   = time.Second
	maydayMaxResponseBytes = 32 << 20
)

// errMaydayResponseTooLarge reports a response body above maydayMaxResponseBytes.
var errMaydayResponseTooLarge = errors.New("Mayday response is too large")

// maydayLocales is the locale enum of the Mayday Public API, which requires a
// locale on every content and collection request. Its "default" value is only
// probed when no other locale holds an article, because a knowledge base
// listed under both would yield every article twice. The order is fixed:
// detected locales are listed in this order.
var maydayLocales = []string{
	"fr-fr", "en-gb", "en-us", "de-de", "es-es", "it-it", "pt-pt", "nl-nl",
	"pl-pl", "ja-jp", "pt-br", "hr-hr", "cs-cz", "hu-hu", "en-in", "es-mx",
	"ro-ro", "ru-ru", "uk-ua", "tr-tr", "sr-rs", "sk-sk",
}

// maydayObjectIDRE matches a Mayday (MongoDB ObjectId) entity ID.
var maydayObjectIDRE = regexp.MustCompile(`^[0-9a-fA-F]{24}$`)

// MaydayConnector reads published Mayday knowledge base articles through the
// Mayday Public API, authenticating with the OAuth 2.0 client credentials grant.
type MaydayConnector struct {
	clientID     string
	clientSecret string
	baseURL      string
	tokenURL     string
	batchSize    int
	locales      []string // configured locales; empty means detect them
	collections  []string // collection IDs or names; empty means all

	tokenMu     sync.Mutex
	cachedToken *maydayToken
	now         func() time.Time
}

// maydayToken is a cached access token and the time it should be refreshed.
type maydayToken struct {
	value     string
	refreshAt time.Time
}

type maydayAPIError struct {
	Status  int
	Message string
}

func (e *maydayAPIError) Error() string {
	return fmt.Sprintf("Mayday API returned HTTP %d: %s", e.Status, e.Message)
}

func maydayStatus(err error) int {
	var apiErr *maydayAPIError
	if errors.As(err, &apiErr) {
		return apiErr.Status
	}
	return 0
}

// maydayPage is the offset-paginated envelope of every Mayday list endpoint.
type maydayPage[T any] struct {
	Data  []T `json:"data"`
	Total int `json:"total"`
	Limit int `json:"limit"`
}

// full reports whether the page holds as many items as the server serves per
// page, so more may follow. The server may cap the requested limit.
func (p maydayPage[T]) full(requested int) bool {
	limit := requested
	if p.Limit > 0 && p.Limit < requested {
		limit = p.Limit
	}
	return len(p.Data) >= limit
}

// nonEmpty reports whether the listing holds at least one item.
func (p maydayPage[T]) nonEmpty() bool {
	return len(p.Data) > 0 || p.Total > 0
}

type maydayContent struct {
	ID            string      `json:"id"`
	SelfcareID    string      `json:"selfcareId"`
	KnowledgeID   string      `json:"knowledgeId"`
	Label         string      `json:"label"`
	Body          string      `json:"body"`
	Type          string      `json:"type"`
	CollectionIDs []string    `json:"collectionsIds"`
	IsOutdated    bool        `json:"isOutdated"`
	UpdatedAt     json.Number `json:"updatedAt"`
	UpdatedAtISO  string      `json:"updatedAtISO"`
}

type maydayCollection struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

// maydayScope is one listing a sync or prune walks: a locale and the
// collection IDs that the configured collection filter maps to in it. A nil
// collectionIDs means every collection.
type maydayScope struct {
	locale        string
	collectionIDs []string
}

// NewMaydayConnector creates a Mayday connector from config.
func NewMaydayConnector(config map[string]any) (*MaydayConnector, error) {
	credentials := configAnyMap(config["credentials"])
	batchSize := configInt(config["sync_batch_size"], 0)
	if batchSize <= 0 {
		batchSize = configInt(config["batch_size"], defaultMaydayBatchSize)
	}
	locales := maydayConfigList(config["languages"])
	for i, locale := range locales {
		locales[i] = strings.ReplaceAll(strings.ToLower(locale), "_", "-")
	}
	return &MaydayConnector{
		clientID:     strings.TrimSpace(stringConfig(credentials["client_id"])),
		clientSecret: strings.TrimSpace(stringConfig(credentials["client_secret"])),
		baseURL:      strings.TrimRight(firstNonEmpty(stringConfig(config["base_url"]), defaultMaydayAPIBaseURL), "/"),
		tokenURL:     strings.TrimRight(firstNonEmpty(stringConfig(config["token_url"]), defaultMaydayTokenURL), "/"),
		batchSize:    batchSize,
		locales:      maydayUnique(locales),
		collections:  maydayUnique(maydayConfigList(config["collections"])),
		now:          time.Now,
	}, nil
}

// maydayConfigList reads a comma-separated string or a JSON list. List items
// are kept whole, so a collection label may contain a comma.
func maydayConfigList(value any) []string {
	var items []string
	switch typed := value.(type) {
	case string:
		return splitCommaList(typed)
	case []string:
		items = typed
	case []any:
		for _, item := range typed {
			items = append(items, stringConfig(item))
		}
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// maydayUnique drops duplicates while keeping the first-seen order.
func maydayUnique(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (c *MaydayConnector) validateConfig() error {
	if c == nil {
		return &ConnectorValidationError{Message: "Mayday connector is nil"}
	}
	if c.clientID == "" || c.clientSecret == "" {
		return &ConnectorMissingCredentialError{Message: "Mayday credentials must include client_id and client_secret"}
	}
	for _, locale := range c.locales {
		if !slices.Contains(maydayLocales, locale) {
			return &ConnectorValidationError{Message: fmt.Sprintf("unsupported Mayday language %q; use a Mayday locale such as fr-fr or en-gb, or leave languages empty to sync every language", locale)}
		}
	}
	for _, endpoint := range []struct{ name, url string }{{"base URL", c.baseURL}, {"token URL", c.tokenURL}} {
		if _, _, err := assertConnectorURLSafeHTTPS(endpoint.url); err != nil {
			return &ConnectorValidationError{Message: fmt.Sprintf("invalid Mayday %s: %v", endpoint.name, err)}
		}
	}
	return nil
}

// Validate checks the config, obtains an access token, and lists one article.
// Without configured languages the probe uses the first Mayday locale; a 400
// there means the account lacks that locale, which still proves the
// credentials, since Mayday authenticates before it validates a request.
func (c *MaydayConnector) Validate(ctx context.Context) error {
	if err := c.validateConfig(); err != nil {
		return err
	}
	locale := maydayLocales[0]
	if len(c.locales) > 0 {
		locale = c.locales[0]
	}
	var page maydayPage[maydayContent]
	err := c.getJSON(ctx, "contents", c.contentsQuery(maydayScope{locale: locale}, 1), &page)
	if err != nil && !(len(c.locales) == 0 && maydayStatus(err) == http.StatusBadRequest) {
		return classifyMaydayError(err)
	}
	return nil
}

// ValidateConnectorSetting validates an unsaved config: the credentials, that
// every collection name resolves, and that the languages and collections
// select at least one published article.
func (c *MaydayConnector) ValidateConnectorSetting(ctx context.Context, request map[string]any) error {
	ctx, cancel := context.WithTimeout(ctx, connectorSettingValidationTimeout)
	defer cancel()
	if err := c.Validate(ctx); err != nil {
		return err
	}
	scopes, err := c.scopes(ctx, 1)
	if err != nil {
		return classifyMaydayError(err)
	}
	found, err := c.anyContent(ctx, scopes)
	if err != nil {
		return classifyMaydayError(err)
	}
	if !found {
		if len(c.locales) == 0 && len(c.collections) == 0 {
			return &ConnectorValidationError{Message: "Mayday returned no published articles in any language."}
		}
		return &ConnectorValidationError{Message: "The configured Mayday languages and collections select no published articles."}
	}
	return nil
}

// classifyMaydayError maps API failures to user-facing validation errors and
// keeps other errors wrapped, so timeouts stay recognisable as transient.
// Response bodies are left out, so a test connection never echoes what an
// arbitrary server returned.
func classifyMaydayError(err error) error {
	var apiErr *maydayAPIError
	if !errors.As(err, &apiErr) {
		var credErr *ConnectorMissingCredentialError
		var valErr *ConnectorValidationError
		if errors.As(err, &credErr) || errors.As(err, &valErr) {
			return err
		}
		return fmt.Errorf("Mayday connection test failed: %w", err)
	}
	switch apiErr.Status {
	case http.StatusUnauthorized:
		return &ConnectorMissingCredentialError{Message: "Mayday rejected the credentials (HTTP 401). Verify the client ID and client secret."}
	case http.StatusForbidden:
		return &ConnectorValidationError{Message: "Mayday credentials lack a required permission (HTTP 403): contents:read, plus collections:read when collections are given by name."}
	case http.StatusNotFound:
		return &ConnectorValidationError{Message: "Mayday API endpoint not found (HTTP 404). Check the API base URL and OAuth token URL."}
	case http.StatusTooManyRequests:
		return &ConnectorValidationError{Message: "Mayday rate limit exceeded (HTTP 429)."}
	default:
		return &ConnectorValidationError{Message: fmt.Sprintf("Mayday API error (HTTP %d).", apiErr.Status)}
	}
}

// accessToken returns the cached access token, requesting a new one when it is
// missing or close to expiry.
func (c *MaydayConnector) accessToken(ctx context.Context) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.cachedToken != nil && c.now().Before(c.cachedToken.refreshAt) {
		return c.cachedToken.value, nil
	}
	token, err := c.requestAccessToken(ctx)
	if err != nil {
		return "", err
	}
	c.cachedToken = &token
	return token.value, nil
}

// invalidateToken drops the cached token if it is still the rejected one, so
// concurrent requests that were refused the same token refresh it only once.
func (c *MaydayConnector) invalidateToken(rejected string) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.cachedToken != nil && c.cachedToken.value == rejected {
		c.cachedToken = nil
	}
}

// requestAccessToken performs the client credentials grant. Mayday expects the
// client ID and secret as HTTP Basic credentials. The request is retried here
// rather than through connectorRequest's retry hook, which would resend an
// already consumed body.
func (c *MaydayConnector) requestAccessToken(ctx context.Context) (maydayToken, error) {
	requestCtx, cancel := context.WithTimeout(ctx, maydayRequestTimeout)
	defer cancel()
	var body []byte
	for attempt := 1; ; attempt++ {
		resp, err := connectorRequest(requestCtx, connectorRequestOptions{
			Method: http.MethodPost,
			RawURL: c.tokenURL,
			Body:   []byte(url.Values{"grant_type": {"client_credentials"}}.Encode()),
			Headers: map[string]string{
				"Authorization": "Basic " + basicAuthHeader(c.clientID, c.clientSecret),
				"Content-Type":  "application/x-www-form-urlencoded",
				"Accept":        "application/json",
			},
			Timeout:  maydayRequestTimeout,
			Validate: assertConnectorURLSafeHTTPS,
		})
		if err != nil {
			return maydayToken{}, fmt.Errorf("Mayday token request failed: %w", connectorUnsafeErr(err))
		}
		body, err = readMaydayBody(resp.Body)
		resp.Body.Close()
		if err != nil {
			return maydayToken{}, err
		}
		if wait, retry := maydayRetryStatus(resp, attempt); retry {
			if err := maydaySleep(requestCtx, wait); err != nil {
				return maydayToken{}, err
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return maydayToken{}, &maydayAPIError{Status: resp.StatusCode, Message: maydayErrorBody(body)}
		}
		break
	}
	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return maydayToken{}, fmt.Errorf("decode Mayday token response: %w", err)
	}
	if payload.AccessToken == "" {
		return maydayToken{}, &ConnectorMissingCredentialError{Message: "Mayday token response did not include an access_token"}
	}
	lifetime := maydayDefaultTokenTTL
	if payload.ExpiresIn > 0 {
		lifetime = time.Duration(min(payload.ExpiresIn, int64(maydayMaxTokenTTL/time.Second))) * time.Second
	}
	skew := min(maydayTokenRefreshSkew, lifetime/2)
	return maydayToken{value: payload.AccessToken, refreshAt: c.now().Add(lifetime - skew)}, nil
}

func maydaySleep(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// getJSON performs an authenticated GET against the Mayday API and decodes the
// JSON response. A 401 is retried once with a fresh token, since Mayday admins
// can revoke credentials or shorten token validity at any time.
func (c *MaydayConnector) getJSON(ctx context.Context, endpoint string, query url.Values, out any) error {
	token, err := c.accessToken(ctx)
	if err != nil {
		return err
	}
	endpointURL := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	if len(query) > 0 {
		endpointURL += "?" + query.Encode()
	}
	status, body, err := c.get(ctx, endpointURL, token)
	if err != nil {
		return err
	}
	if status == http.StatusUnauthorized {
		c.invalidateToken(token)
		if token, err = c.accessToken(ctx); err != nil {
			return err
		}
		if status, body, err = c.get(ctx, endpointURL, token); err != nil {
			return err
		}
	}
	if status < 200 || status >= 300 {
		return &maydayAPIError{Status: status, Message: maydayErrorBody(body)}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode Mayday %s response: %w", endpoint, err)
	}
	return nil
}

// get sends one GET, retrying rate-limited and unavailable responses.
func (c *MaydayConnector) get(ctx context.Context, endpointURL, token string) (int, []byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, maydayRequestTimeout)
	defer cancel()
	resp, err := connectorRequest(requestCtx, connectorRequestOptions{
		Method: http.MethodGet,
		RawURL: endpointURL,
		Headers: map[string]string{
			"Authorization": "Bearer " + token,
			"Accept":        "application/json",
		},
		Timeout:     maydayRequestTimeout,
		Validate:    assertConnectorURLSafeHTTPS,
		RetryStatus: maydayRetryStatus,
	})
	if err != nil {
		return 0, nil, connectorUnsafeErr(err)
	}
	defer resp.Body.Close()
	body, err := readMaydayBody(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

// maydayRetryStatus decides whether to retry a response: rate limits up to
// maydayMaxRateLimitRetries times and server errors up to maydayMaxRetries
// times, with exponential backoff unless the server sends Retry-After.
func maydayRetryStatus(resp *http.Response, attempt int) (time.Duration, bool) {
	var retries int
	var wait time.Duration
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		retries, wait = maydayMaxRateLimitRetries, 5*maydayRetryBaseDelay
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		retries, wait = maydayMaxRetries, maydayRetryBaseDelay
	default:
		return 0, false
	}
	if attempt > retries {
		return 0, false
	}
	wait <<= attempt - 1
	if raw := strings.TrimSpace(resp.Header.Get("Retry-After")); raw != "" {
		if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil && seconds >= 0 {
			wait = time.Duration(min(seconds, int64(maydayMaxRetryWait/time.Second))) * time.Second
		} else if at, err := http.ParseTime(raw); err == nil {
			wait = max(time.Until(at), 0)
		}
	}
	return min(wait, maydayMaxRetryWait), true
}

// readMaydayBody reads a response body, failing instead of truncating one that
// exceeds maydayMaxResponseBytes.
func readMaydayBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, int64(maydayMaxResponseBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("read Mayday response: %w", err)
	}
	if len(body) > maydayMaxResponseBytes {
		return nil, errMaydayResponseTooLarge
	}
	return body, nil
}

// maydayErrorBody shortens an error response body for messages and logs.
func maydayErrorBody(body []byte) string {
	message := strings.TrimSpace(string(body))
	if len(message) > maydayMaxErrorBytes {
		message = strings.ToValidUTF8(message[:maydayMaxErrorBytes], "") + "..."
	}
	return message
}

// contentsQuery builds a GET /contents request for a scope's published
// articles, oldest update first.
func (c *MaydayConnector) contentsQuery(scope maydayScope, limit int) url.Values {
	query := url.Values{
		"locale":        {scope.locale},
		"types[]":       {maydayContentType},
		"isPublished":   {"true"},
		"page":          {"1"},
		"limit":         {strconv.Itoa(limit)},
		"sortKey":       {"updatedAt"},
		"sortDirection": {"1"},
	}
	for _, id := range scope.collectionIDs {
		query.Add("collectionIds[]", id)
	}
	return query
}

// scopes resolves what a sync or prune lists. Configured locales are used as
// given; otherwise every locale holding a published article is detected,
// running detection detectRuns times and keeping a locale any run found, so
// one transiently empty answer cannot drop it. The collection filter is then
// mapped onto each locale's collection IDs: IDs pass through, names match that
// locale's collection labels case-insensitively.
//
// A configured locale whose labels lack a configured name is an error. With
// detected locales a name only has to exist in one of them, and a locale where
// the filter matches no collection is skipped.
func (c *MaydayConnector) scopes(ctx context.Context, detectRuns int) ([]maydayScope, error) {
	explicit := len(c.locales) > 0
	locales := c.locales
	if !explicit {
		found := map[string]struct{}{}
		for range max(detectRuns, 1) {
			detected, err := c.detectLocales(ctx)
			if err != nil {
				return nil, err
			}
			for _, locale := range detected {
				found[locale] = struct{}{}
			}
		}
		for _, locale := range append(slices.Clone(maydayLocales), maydayFallbackLocale) {
			if _, ok := found[locale]; ok {
				locales = append(locales, locale)
			}
		}
	}
	var ids, names []string
	for _, entry := range c.collections {
		if maydayObjectIDRE.MatchString(entry) {
			ids = append(ids, entry)
		} else {
			names = append(names, entry)
		}
	}
	unmatched := make(map[string]struct{}, len(names))
	for _, name := range names {
		unmatched[name] = struct{}{}
	}
	scopes := make([]maydayScope, 0, len(locales))
	for _, locale := range locales {
		if len(c.collections) == 0 {
			scopes = append(scopes, maydayScope{locale: locale})
			continue
		}
		collectionIDs := slices.Clone(ids)
		if len(names) > 0 {
			byLabel, err := c.collectionIDsByLabel(ctx, locale)
			if err != nil {
				return nil, err
			}
			for _, name := range names {
				matched, ok := byLabel[strings.ToLower(name)]
				if !ok {
					if explicit {
						return nil, &ConnectorValidationError{Message: fmt.Sprintf("Mayday collection %q was not found in language %q", name, locale)}
					}
					continue
				}
				collectionIDs = append(collectionIDs, matched...)
				delete(unmatched, name)
			}
		}
		if len(collectionIDs) == 0 {
			continue
		}
		scopes = append(scopes, maydayScope{locale: locale, collectionIDs: maydayUnique(collectionIDs)})
	}
	if !explicit && len(locales) > 0 {
		for _, name := range names {
			if _, ok := unmatched[name]; ok {
				return nil, &ConnectorValidationError{Message: fmt.Sprintf("Mayday collection %q was not found in any language", name)}
			}
		}
	}
	return scopes, nil
}

// detectLocales returns, in maydayLocales order, every locale holding at least
// one published article, falling back to Mayday's "default" locale when none
// does. A 400 for a locale means the account does not use it. The probes run
// concurrently so test connection stays within its timeout.
func (c *MaydayConnector) detectLocales(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	found := make([]bool, len(maydayLocales))
	errs := make([]error, len(maydayLocales))
	slots := make(chan struct{}, maydayDetectWorkers)
	var wg sync.WaitGroup
	for i, locale := range maydayLocales {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-slots }()
			found[i], errs[i] = c.localeHasContent(ctx, locale)
			if errs[i] != nil {
				cancel()
			}
		}()
	}
	wg.Wait()
	if err := maydayFirstError(errs); err != nil {
		return nil, err
	}
	var locales []string
	for i, locale := range maydayLocales {
		if found[i] {
			locales = append(locales, locale)
		}
	}
	if len(locales) > 0 {
		return locales, nil
	}
	ok, err := c.localeHasContent(ctx, maydayFallbackLocale)
	if err != nil || !ok {
		return nil, err
	}
	return []string{maydayFallbackLocale}, nil
}

func (c *MaydayConnector) localeHasContent(ctx context.Context, locale string) (bool, error) {
	var page maydayPage[maydayContent]
	err := c.getJSON(ctx, "contents", c.contentsQuery(maydayScope{locale: locale}, 1), &page)
	if maydayStatus(err) == http.StatusBadRequest {
		return false, nil
	}
	return err == nil && page.nonEmpty(), err
}

// maydayFirstError returns the first error that is not a cancellation caused
// by another failure, falling back to the first error.
func maydayFirstError(errs []error) error {
	var first error
	for _, err := range errs {
		if err == nil {
			continue
		}
		if !errors.Is(err, context.Canceled) {
			return err
		}
		if first == nil {
			first = err
		}
	}
	return first
}

// anyContent reports whether any scope lists at least one article.
func (c *MaydayConnector) anyContent(ctx context.Context, scopes []maydayScope) (bool, error) {
	for _, scope := range scopes {
		var page maydayPage[maydayContent]
		if err := c.getJSON(ctx, "contents", c.contentsQuery(scope, 1), &page); err != nil {
			return false, err
		}
		if page.nonEmpty() {
			return true, nil
		}
	}
	return false, nil
}

// collectionIDsByLabel lists one locale's collections, keyed by lower-cased
// label. Labels are not unique, so a label can map to several collections.
// Entries without a valid ID or with an oversized label are ignored, and the
// listing is bounded so a misbehaving server cannot exhaust memory.
func (c *MaydayConnector) collectionIDsByLabel(ctx context.Context, locale string) (map[string][]string, error) {
	byLabel := map[string][]string{}
	listed := 0
	for page := 1; page <= maydayMaxCollectionPages; page++ {
		query := url.Values{
			"locale": {locale},
			"page":   {strconv.Itoa(page)},
			"limit":  {strconv.Itoa(maydayPageSize)},
		}
		var resp maydayPage[maydayCollection]
		if err := c.getJSON(ctx, "collections", query, &resp); err != nil {
			return nil, err
		}
		listed += len(resp.Data)
		for _, collection := range resp.Data {
			label := strings.ToLower(strings.TrimSpace(collection.Label))
			if maydayObjectIDRE.MatchString(collection.ID) && label != "" && len(label) <= maydayMaxLabelBytes {
				byLabel[label] = append(byLabel[label], collection.ID)
			}
		}
		if len(resp.Data) == 0 || (!resp.full(maydayPageSize) && listed >= resp.Total) {
			return byLabel, nil
		}
	}
	return nil, fmt.Errorf("Mayday collections of language %q exceed %d pages", locale, maydayMaxCollectionPages)
}

// maydayLister walks one scope's contents in updatedAt order, using the
// timestamp of the last item read as the next lower bound. Unlike page
// offsets, the walk never skips an article that exists for its whole
// duration: deleting an article does not move the others, and an article
// edited during the walk moves past the bound, where the walk reaches it again.
//
// Each request is a first page, so a page is followed by more whenever it is
// full or the reported total exceeds it. Pages out of updatedAt order or
// outside the requested bounds fail the walk instead of truncating it, and a
// page too large to read is requested again at half the size.
type maydayLister struct {
	connector *MaydayConnector
	scope     maydayScope
	limit     int
	lower     int64 // updatedAt lower bound in Unix milliseconds
	bounded   bool  // lower applies
	strict    bool  // lower is exclusive
	upper     int64 // inclusive updatedAt upper bound; 0 means none
	atLower   map[string]struct{}
	tie       *maydayTiePass
	tiePasses int
	done      bool
}

// maydayTiePass reads, by offset, the contents sharing the bound's updatedAt
// when they fill a whole page and the bound cannot move. Offset pages can skip
// items when the set changes or its order is unstable, so IDs are collected
// across passes and the instant is left only once they reach the reported
// total. A past instant can lose members but never gain any, so while the
// total stays constant the collected IDs are exactly its members.
type maydayTiePass struct {
	page  int
	total int
	ids   map[string]struct{}
}

func newMaydayLister(connector *MaydayConnector, scope maydayScope) *maydayLister {
	return &maydayLister{connector: connector, scope: scope, limit: maydayPageSize}
}

// next returns the next contents not returned before, or nil once the walk is
// complete.
func (l *maydayLister) next(ctx context.Context) ([]maydayContent, error) {
	for !l.done {
		query := l.connector.contentsQuery(l.scope, l.limit)
		switch {
		case l.tie != nil:
			instant := maydayFormatMillis(l.lower)
			query.Set("updatedAt[gte]", instant)
			query.Set("updatedAt[lte]", instant)
			query.Set("page", strconv.Itoa(l.tie.page))
		case l.bounded && l.strict:
			query.Set("updatedAt[gt]", maydayFormatMillis(l.lower))
		case l.bounded:
			query.Set("updatedAt[gte]", maydayFormatMillis(l.lower))
		}
		if l.upper > 0 && l.tie == nil {
			query.Set("updatedAt[lte]", maydayFormatMillis(l.upper))
		}
		var resp maydayPage[maydayContent]
		if err := l.connector.getJSON(ctx, "contents", query, &resp); err != nil {
			if errors.Is(err, errMaydayResponseTooLarge) && l.limit > 1 {
				l.limit /= 2
				if l.tie != nil {
					l.tie.page = 1
				}
				continue
			}
			return nil, err
		}
		if err := l.checkPage(resp.Data); err != nil {
			return nil, err
		}
		fresh := make([]maydayContent, 0, len(resp.Data))
		for _, content := range resp.Data {
			if l.unseen(content) {
				fresh = append(fresh, content)
			}
		}

		switch {
		case l.tie != nil:
			l.remember(fresh)
			if err := l.advanceTie(resp); err != nil {
				return nil, err
			}
		case len(resp.Data) == 0, !resp.full(l.limit) && resp.Total <= len(resp.Data):
			l.done = true
		default:
			last := maydayUpdatedAtMillis(resp.Data[len(resp.Data)-1])
			if last <= 0 {
				return nil, fmt.Errorf("Mayday returned a page of contents without updatedAt for language %q", l.scope.locale)
			}
			if l.bounded && last == l.lower {
				l.remember(fresh)
				if !resp.full(l.limit) {
					// A short page holds the whole instant; the total
					// only overstated what follows it.
					l.strict, l.atLower = true, nil
					break
				}
				// A full page sharing one updatedAt cannot move the
				// bound; read that instant by offset instead.
				l.tie = &maydayTiePass{page: 1, total: -1, ids: map[string]struct{}{}}
				break
			}
			l.lower, l.bounded, l.strict = last, true, false
			l.atLower = map[string]struct{}{}
			for _, content := range resp.Data {
				if maydayUpdatedAtMillis(content) == last {
					l.atLower[content.ID] = struct{}{}
				}
			}
		}
		if len(fresh) > 0 {
			return fresh, nil
		}
	}
	return nil, nil
}

// checkPage rejects a page the server did not sort or filter as requested, or
// whose contents lack a usable ID, so an unexpected API change fails the walk
// rather than truncating it.
func (l *maydayLister) checkPage(contents []maydayContent) error {
	previous := int64(-1)
	for _, content := range contents {
		if content.ID == "" || len(content.ID) > maydayMaxIDBytes {
			return fmt.Errorf("Mayday returned a content without a valid id for language %q", l.scope.locale)
		}
		updatedAt := maydayUpdatedAtMillis(content)
		outside := updatedAt < previous
		switch {
		case l.tie != nil:
			outside = outside || updatedAt != l.lower
		case l.bounded:
			outside = outside || updatedAt < l.lower || (l.strict && updatedAt == l.lower)
		}
		if l.upper > 0 && updatedAt > l.upper {
			outside = true
		}
		if outside {
			return fmt.Errorf("Mayday returned contents for language %q out of the requested updatedAt order or bounds", l.scope.locale)
		}
		previous = updatedAt
	}
	return nil
}

// advanceTie records one tie page, then moves to the next page, starts another
// pass, or leaves the instant once every member has been read.
func (l *maydayLister) advanceTie(resp maydayPage[maydayContent]) error {
	pass := l.tie
	if resp.Total > maydayMaxTieTotal {
		return fmt.Errorf("Mayday lists %d contents of language %q updated at the same instant, more than %d", resp.Total, l.scope.locale, maydayMaxTieTotal)
	}
	if pass.total >= 0 && resp.Total != pass.total {
		// Members left the instant: what was collected may no longer
		// match the total, so collect again.
		pass.ids = map[string]struct{}{}
		pass.page = 0
		if err := l.countTiePass(); err != nil {
			return err
		}
	}
	pass.total = resp.Total
	for _, content := range resp.Data {
		pass.ids[content.ID] = struct{}{}
	}
	if len(pass.ids) >= pass.total {
		l.tie, l.tiePasses = nil, 0
		l.strict, l.atLower = true, nil
		return nil
	}
	if len(resp.Data) > 0 && pass.page < pass.total {
		pass.page++
		return nil
	}
	pass.page = 1
	return l.countTiePass()
}

func (l *maydayLister) countTiePass() error {
	l.tiePasses++
	if l.tiePasses > maydayMaxTiePasses {
		return fmt.Errorf("Mayday contents of language %q updated at %s kept changing while being listed", l.scope.locale, maydayFormatMillis(l.lower))
	}
	return nil
}

// unseen reports whether content lies beyond the walk's position.
func (l *maydayLister) unseen(content maydayContent) bool {
	if !l.bounded || maydayUpdatedAtMillis(content) != l.lower {
		return true
	}
	_, seen := l.atLower[content.ID]
	return !seen
}

func (l *maydayLister) remember(contents []maydayContent) {
	if l.atLower == nil {
		l.atLower = map[string]struct{}{}
	}
	for _, content := range contents {
		l.atLower[content.ID] = struct{}{}
	}
}

func maydayFormatMillis(millis int64) string {
	return time.UnixMilli(millis).UTC().Format(maydayTimeFilterLayout)
}

func maydaySourceID(locale, contentID string) string {
	return maydaySourceIDPrefix + locale + "_" + contentID
}

// maydayUpdatedAtMillis reads the Unix-millisecond updatedAt, falling back to
// its ISO 8601 twin, or 0 when neither parses.
func maydayUpdatedAtMillis(content maydayContent) int64 {
	if millis, err := content.UpdatedAt.Int64(); err == nil && millis > 0 {
		return millis
	}
	if millis, err := content.UpdatedAt.Float64(); err == nil && millis > 0 {
		return int64(millis)
	}
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(content.UpdatedAtISO)); err == nil {
		return parsed.UnixMilli()
	}
	return 0
}

// maydayArticleText converts an article body to Markdown, or returns "" when
// it holds no text. Sync and prune share it, so an article that loses its text
// is removed rather than kept with its old content.
func maydayArticleText(body string) string {
	text, err := newMarkdownConverter().ConvertString(body)
	if err != nil {
		text = htmlToText(body)
	}
	return strings.TrimSpace(text)
}

// contentDocument converts one Mayday article into a Markdown source document.
// It reports false for articles without text. Every metadata key is always
// set, since some document engines merge metadata updates instead of replacing
// them; is_outdated is the text "true" or "false" so a metadata filter can
// match it with "=". The fingerprint covers the metadata as well as the text,
// so a metadata change such as an article being marked outdated updates the
// document. With several locales synced, the locale is added to the name so
// the language versions of an article can be told apart.
func contentDocument(locale string, content maydayContent, nameLocale bool) (SourceDocument, bool) {
	body := maydayArticleText(content.Body)
	if body == "" {
		return SourceDocument{}, false
	}
	title := strings.TrimSpace(content.Label)
	text := body
	if title != "" {
		text = "# " + title + "\n\n" + body
	}
	name := firstNonEmpty(title, content.ID)
	if nameLocale {
		name += " (" + locale + ")"
	}
	blob := []byte(text)
	collectionIDs := content.CollectionIDs
	if collectionIDs == nil {
		collectionIDs = []string{}
	}
	var updatedAt time.Time
	if millis := maydayUpdatedAtMillis(content); millis > 0 {
		updatedAt = time.UnixMilli(millis).UTC()
	}
	metadata := map[string]any{
		"content_id":     content.ID,
		"locale":         locale,
		"content_type":   firstNonEmpty(content.Type, maydayContentType),
		"knowledge_id":   content.KnowledgeID,
		"selfcare_id":    content.SelfcareID,
		"collection_ids": collectionIDs,
		"is_outdated":    strconv.FormatBool(content.IsOutdated),
	}
	return SourceDocument{
		SourceID:           maydaySourceID(locale, content.ID),
		SemanticIdentifier: name,
		Extension:          ".md",
		Blob:               blob,
		UpdatedAt:          updatedAt,
		SizeBytes:          int64(len(blob)),
		Metadata:           metadata,
		Fingerprint:        stableFingerprint(map[string]any{"text": text, "metadata": metadata}),
	}, true
}

// maydayScopeKey identifies a scope list, so a resume checkpoint is only
// honoured while the languages and collections it was taken under still apply.
func maydayScopeKey(scopes []maydayScope) string {
	parts := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		parts = append(parts, scope.locale+"="+strings.Join(scope.collectionIDs, ","))
	}
	return contentFingerprint([]byte(strings.Join(parts, ";")))
}

func maydayMultiLocale(scopes []maydayScope) bool {
	for _, scope := range scopes {
		if scope.locale != scopes[0].locale {
			return true
		}
	}
	return false
}

// OpenSync opens a Mayday synchronization session.
func (c *MaydayConnector) OpenSync(ctx context.Context, request SyncRequest) (SyncSession, error) {
	if err := c.validateConfig(); err != nil {
		return nil, err
	}
	scopes, err := c.scopes(ctx, 1)
	if err != nil {
		return nil, err
	}
	session := &maydaySyncSession{
		connector:  c,
		request:    request,
		scopes:     scopes,
		scopeKey:   maydayScopeKey(scopes),
		nameLocale: maydayMultiLocale(scopes),
	}
	if err := session.applyResume(request.Resume); err != nil {
		return nil, err
	}
	return session, nil
}

// maydaySyncCursor records the last committed article. Scopes are walked in
// order and each in updatedAt order, so a resumed session restarts the cursor's
// scope at that article's updatedAt and skips the article itself.
type maydaySyncCursor struct {
	Scopes    string `json:"scopes"`
	Locale    string `json:"locale"`
	UpdatedAt int64  `json:"updated_at"`
	ContentID string `json:"content_id"`
}

type maydaySyncSession struct {
	connector  *MaydayConnector
	request    SyncRequest
	scopes     []maydayScope
	scopeKey   string
	nameLocale bool
	done       bool

	scopeIndex  int
	lister      *maydayLister
	buffer      []maydayContent
	bufferIndex int

	last   maydaySyncCursor
	resume *maydaySyncCursor
}

func (s *maydaySyncSession) applyResume(checkpoint *SyncCheckpoint) error {
	if checkpoint == nil {
		return nil
	}
	if checkpoint.Cursor == "" {
		return fmt.Errorf("mayday sync cursor is missing: %w", ErrSyncResumeInvalid)
	}
	var cursor maydaySyncCursor
	if err := json.Unmarshal([]byte(checkpoint.Cursor), &cursor); err != nil {
		return fmt.Errorf("mayday sync cursor is invalid: %w", ErrSyncResumeInvalid)
	}
	if cursor.ContentID == "" || cursor.UpdatedAt <= 0 {
		return fmt.Errorf("mayday sync checkpoint has no valid anchor: %w", ErrSyncResumeInvalid)
	}
	if cursor.Scopes != s.scopeKey {
		return fmt.Errorf("mayday languages or collections changed since the checkpoint: %w", ErrSyncResumeInvalid)
	}
	scopeIndex := slices.IndexFunc(s.scopes, func(scope maydayScope) bool { return scope.locale == cursor.Locale })
	if scopeIndex < 0 {
		return fmt.Errorf("mayday sync checkpoint language %q is no longer synced: %w", cursor.Locale, ErrSyncResumeInvalid)
	}
	s.scopeIndex = scopeIndex
	s.resume = &cursor
	return nil
}

func (s *maydaySyncSession) NextBatch(ctx context.Context) (SyncBatch, error) {
	if s.done {
		return SyncBatch{}, io.EOF
	}
	batchSize := s.connector.batchSize
	documents := make([]SourceDocument, 0, batchSize)
	for len(documents) < batchSize {
		doc, ok, err := s.nextDocument(ctx)
		if err != nil {
			return SyncBatch{}, err
		}
		if !ok {
			break
		}
		documents = append(documents, doc)
	}
	if len(documents) == 0 {
		return SyncBatch{}, io.EOF
	}
	return SyncBatch{Documents: documents, Checkpoint: s.checkpoint(documents[len(documents)-1])}, nil
}

func (s *maydaySyncSession) Close() error {
	return nil
}

func (s *maydaySyncSession) nextDocument(ctx context.Context) (SourceDocument, bool, error) {
	for {
		if s.bufferIndex >= len(s.buffer) {
			if s.scopeIndex >= len(s.scopes) {
				s.done = true
				return SourceDocument{}, false, nil
			}
			if s.lister == nil {
				s.lister = s.newLister()
			}
			contents, err := s.lister.next(ctx)
			if err != nil {
				return SourceDocument{}, false, err
			}
			if len(contents) == 0 {
				s.lister = nil
				s.scopeIndex++
				continue
			}
			s.buffer, s.bufferIndex = contents, 0
			continue
		}
		content := s.buffer[s.bufferIndex]
		s.bufferIndex++
		locale := s.scopes[s.scopeIndex].locale
		// The lister's bounds already confine contents to the window
		// (WindowStart < updatedAt <= WindowEnd), and checkPage rejects any
		// page the server did not filter.
		doc, ok := contentDocument(locale, content, s.nameLocale)
		if !ok {
			continue
		}
		s.last = maydaySyncCursor{Scopes: s.scopeKey, Locale: locale, UpdatedAt: maydayUpdatedAtMillis(content), ContentID: content.ID}
		return doc, true, nil
	}
}

// newLister starts the walk of the current scope. It is bounded above by the
// window end even for a full sync, so an article edited during the sync is
// left to the next window instead of being emitted twice; it is bounded below
// by the window start and, for the scope a resume points into, by the
// committed position.
func (s *maydaySyncSession) newLister() *maydayLister {
	lister := newMaydayLister(s.connector, s.scopes[s.scopeIndex])
	if end := s.request.WindowEnd; !end.IsZero() {
		lister.upper = end.UnixMilli()
	}
	if start := s.request.WindowStart; !s.request.FromBeginning && start != nil {
		lister.lower, lister.bounded, lister.strict = start.UnixMilli(), true, true
	}
	if resume := s.resume; resume != nil {
		lister.lower, lister.bounded, lister.strict = resume.UpdatedAt, true, false
		lister.atLower = map[string]struct{}{resume.ContentID: {}}
		s.resume = nil
	}
	return lister
}

func (s *maydaySyncSession) checkpoint(last SourceDocument) *SyncCheckpoint {
	data, err := json.Marshal(s.last)
	if err != nil {
		return nil
	}
	updatedAt := last.UpdatedAt
	return &SyncCheckpoint{Cursor: string(data), SourceID: last.SourceID, UpdatedAt: &updatedAt}
}

// OpenPrune opens a complete prune snapshot session over the same scopes the
// sync indexes, so articles that are deleted, unpublished, moved out of the
// configured collections, or left without text are removed.
//
// Detected locales are detected twice, so one transiently empty answer cannot
// drop a language. When no scope lists an article the prune is skipped, so an
// outage that returns empty listings cannot empty the dataset.
func (c *MaydayConnector) OpenPrune(ctx context.Context, request PruneRequest) (PruneSession, error) {
	if err := c.validateConfig(); err != nil {
		return nil, err
	}
	scopes, err := c.scopes(ctx, 2)
	if err != nil {
		return nil, err
	}
	found, err := c.anyContent(ctx, scopes)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Mayday lists no published articles for the configured languages and collections, so the prune is skipped: %w", ErrPruneUnsupported)
	}
	return &maydayPruneSession{connector: c, scopes: scopes}, nil
}

// maydayPruneSession lists every scope for the prune snapshot. A scope that
// lists nothing is walked once more before its documents are given up, and a
// snapshot that ends empty although OpenPrune found articles fails instead of
// pruning everything.
type maydayPruneSession struct {
	connector  *MaydayConnector
	scopes     []maydayScope
	scopeIndex int
	lister     *maydayLister
	scopeItems int
	rewalked   bool
	listed     int
}

// NextBatch returns the next listed articles as a slim snapshot batch.
func (s *maydayPruneSession) NextBatch(ctx context.Context) (PruneBatch, error) {
	for s.scopeIndex < len(s.scopes) {
		scope := s.scopes[s.scopeIndex]
		if s.lister == nil {
			s.lister = newMaydayLister(s.connector, scope)
		}
		contents, err := s.lister.next(ctx)
		if err != nil {
			return PruneBatch{}, err
		}
		if len(contents) == 0 {
			s.lister = nil
			if s.scopeItems == 0 && !s.rewalked {
				s.rewalked = true
				continue
			}
			s.scopeIndex++
			s.scopeItems, s.rewalked = 0, false
			continue
		}
		documents := make([]SlimDocument, 0, len(contents))
		for _, content := range contents {
			s.scopeItems++
			if maydayArticleText(content.Body) == "" {
				continue
			}
			documents = append(documents, SlimDocument{SourceID: maydaySourceID(scope.locale, content.ID)})
		}
		if len(documents) > 0 {
			s.listed += len(documents)
			return PruneBatch{Documents: documents}, nil
		}
	}
	if s.listed == 0 {
		return PruneBatch{}, fmt.Errorf("Mayday listed no articles with text for the prune snapshot although articles exist")
	}
	return PruneBatch{}, io.EOF
}

func (s *maydayPruneSession) Close() error {
	return nil
}
