package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/metadata"

	"github.com/agentledger/agentledger/services/internal/shared/config"
	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

var (
	errAuthHeaderMissing             = errors.New("authorization header is required")
	errAuthHeaderInvalid             = errors.New("authorization header must use Bearer token")
	errRequiredTenantWorkspaceClaims = errors.New("jwt missing required tenant/workspace claims")
)

var allowedJWTAlgs = []string{
	"RS256",
	"RS384",
	"RS512",
	"PS256",
	"PS384",
	"PS512",
	"ES256",
	"ES384",
	"ES512",
	"EdDSA",
}

var subjectClaimAliases = []string{"sub", "subject"}
var issuerClaimAliases = []string{"iss", "issuer"}
var audienceClaimAliases = []string{"aud", "audience"}
var scopeClaimAliases = []string{"scope", "scp", "scopes"}
var tenantClaimAliases = []string{"tenant_id", "tenantid", "tenant", "tid", "org_id"}
var workspaceClaimAliases = []string{"workspace_id", "workspaceid", "workspace", "wid"}

type authClaims struct {
	Subject     string
	Issuer      string
	Audience    []string
	Scope       string
	TenantID    string
	WorkspaceID string
}

func (c authClaims) audienceString() string {
	return strings.Join(c.Audience, ",")
}

type claimResolution struct {
	MatchedAlias  string `json:"matched_alias,omitempty"`
	MatchedPath   string `json:"matched_path,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
}

type authClaimsResolution struct {
	Subject     claimResolution `json:"subject"`
	Issuer      claimResolution `json:"issuer"`
	Audience    claimResolution `json:"audience"`
	Scope       claimResolution `json:"scope"`
	TenantID    claimResolution `json:"tenant_id"`
	WorkspaceID claimResolution `json:"workspace_id"`
}

type authAuditInfo struct {
	ClaimResolution authClaimsResolution `json:"claim_resolution"`
	FailureReason   string               `json:"failure_reason,omitempty"`
}

type claimExtractionTrace struct {
	MatchedAlias  string
	MatchedPath   string
	FailureReason string
}

type jwtAuthenticator struct {
	issuer   string
	audience string
	jwksURI  string
	jwks     keyfunc.Keyfunc
}

func newJWTAuthenticator(ctx context.Context, cfg config.Config) (*jwtAuthenticator, error) {
	issuer := strings.TrimSpace(cfg.OIDC.Issuer)
	audience := strings.TrimSpace(cfg.OIDC.Audience)
	jwksURI := strings.TrimSpace(cfg.OIDC.JWKSURI)

	if issuer == "" {
		return nil, fmt.Errorf("missing jwt issuer config (OIDC_ISSUER)")
	}
	if audience == "" {
		return nil, fmt.Errorf("missing jwt audience config (OIDC_AUDIENCE)")
	}
	if jwksURI == "" {
		return nil, fmt.Errorf("missing jwt jwks uri config (OIDC_JWKS_URI)")
	}

	jwks, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURI})
	if err != nil {
		return nil, fmt.Errorf("create jwks keyfunc: %w", err)
	}

	return &jwtAuthenticator{
		issuer:   issuer,
		audience: audience,
		jwksURI:  jwksURI,
		jwks:     jwks,
	}, nil
}

func (a *jwtAuthenticator) AuthenticateHTTP(r *http.Request) (authClaims, authAuditInfo, error) {
	if a == nil {
		err := errors.New("authenticator is nil")
		return authClaims{}, authAuditInfo{FailureReason: err.Error()}, err
	}

	token, err := extractBearerToken(r.Header.Get("Authorization"))
	if err != nil {
		return authClaims{}, authAuditInfo{FailureReason: err.Error()}, err
	}

	return a.verifyToken(token)
}

func (a *jwtAuthenticator) AuthenticateGRPC(ctx context.Context) (authClaims, authAuditInfo, error) {
	if a == nil {
		err := errors.New("authenticator is nil")
		return authClaims{}, authAuditInfo{FailureReason: err.Error()}, err
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return authClaims{}, authAuditInfo{FailureReason: errAuthHeaderMissing.Error()}, errAuthHeaderMissing
	}

	token, err := extractBearerTokenFromValues(md.Get("authorization"))
	if err != nil {
		return authClaims{}, authAuditInfo{FailureReason: err.Error()}, err
	}

	return a.verifyToken(token)
}

func (a *jwtAuthenticator) verifyToken(tokenString string) (authClaims, authAuditInfo, error) {
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		a.jwtKeyfunc,
		jwt.WithIssuer(a.issuer),
		jwt.WithAudience(a.audience),
		jwt.WithValidMethods(allowedJWTAlgs),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		failureReason := fmt.Sprintf("verify jwt failed: %v", err)
		return authClaims{}, authAuditInfo{FailureReason: failureReason}, fmt.Errorf("verify jwt failed: %w", err)
	}
	if token == nil || !token.Valid {
		err := errors.New("token is invalid")
		return authClaims{}, authAuditInfo{FailureReason: err.Error()}, err
	}

	auth, resolution := extractAuthClaimsWithResolution(map[string]any(claims))
	auditInfo := authAuditInfo{ClaimResolution: resolution}
	if err := validateRequiredTenantWorkspaceClaims(auth); err != nil {
		auditInfo.FailureReason = err.Error()
		return auth, auditInfo, err
	}
	return auth, auditInfo, nil
}

func (a *jwtAuthenticator) jwtKeyfunc(token *jwt.Token) (any, error) {
	if token == nil || token.Method == nil {
		return nil, errors.New("missing jwt signing method")
	}
	if strings.EqualFold(token.Method.Alg(), "none") {
		return nil, errors.New("jwt alg none is not allowed")
	}
	return a.jwks.Keyfunc(token)
}

func extractBearerTokenFromValues(values []string) (string, error) {
	if len(values) == 0 {
		return "", errAuthHeaderMissing
	}

	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		token, err := extractBearerToken(value)
		if err == nil {
			return token, nil
		}
	}

	return "", errAuthHeaderInvalid
}

func extractBearerToken(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", errAuthHeaderMissing
	}

	parts := strings.Fields(trimmed)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", errAuthHeaderInvalid
	}
	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", errAuthHeaderInvalid
	}
	return token, nil
}

func applyAuthClaimsToBatch(batch *ingest.IngestBatch, claims authClaims) {
	if batch == nil {
		return
	}

	if tenantID := strings.TrimSpace(claims.TenantID); tenantID != "" {
		batch.Agent.TenantID = tenantID
	}
	if workspaceID := strings.TrimSpace(claims.WorkspaceID); workspaceID != "" {
		batch.Agent.WorkspaceID = workspaceID
	}

	authMetadata := map[string]string{
		"auth.subject":  claims.Subject,
		"auth.issuer":   claims.Issuer,
		"auth.audience": claims.audienceString(),
		"auth.scope":    claims.Scope,
	}

	if batch.Metadata == nil {
		batch.Metadata = make(map[string]string, len(authMetadata))
	}
	for key, value := range authMetadata {
		batch.Metadata[key] = value
	}

	for i := range batch.Events {
		if batch.Events[i].Metadata == nil {
			batch.Events[i].Metadata = make(map[string]string, len(authMetadata))
		}
		for key, value := range authMetadata {
			// 服务端可信鉴权信息必须覆盖同名客户端字段。
			batch.Events[i].Metadata[key] = value
		}
	}
}

func validateRequiredTenantWorkspaceClaims(claims authClaims) error {
	if strings.TrimSpace(claims.TenantID) == "" || strings.TrimSpace(claims.WorkspaceID) == "" {
		return errRequiredTenantWorkspaceClaims
	}
	return nil
}

func extractAuthClaims(claims map[string]any) authClaims {
	auth, _ := extractAuthClaimsWithResolution(claims)
	return auth
}

func extractAuthClaimsWithResolution(claims map[string]any) (authClaims, authClaimsResolution) {
	subjectValues, subjectTrace := extractClaimStringsWithTrace(claims, subjectClaimAliases)
	issuerValues, issuerTrace := extractClaimStringsWithTrace(claims, issuerClaimAliases)
	audienceValues, audienceTrace := extractClaimStringsWithTrace(claims, audienceClaimAliases)
	scopeValues, scopeTrace := extractClaimStringsWithTrace(claims, scopeClaimAliases)
	tenantValues, tenantTrace := extractClaimStringsWithTrace(claims, tenantClaimAliases)
	workspaceValues, workspaceTrace := extractClaimStringsWithTrace(claims, workspaceClaimAliases)

	auth := authClaims{
		Subject:     firstString(subjectValues),
		Issuer:      firstString(issuerValues),
		Audience:    audienceValues,
		Scope:       strings.Join(scopeValues, " "),
		TenantID:    firstString(tenantValues),
		WorkspaceID: firstString(workspaceValues),
	}
	resolution := authClaimsResolution{
		Subject:     claimResolutionFromTrace(subjectTrace),
		Issuer:      claimResolutionFromTrace(issuerTrace),
		Audience:    claimResolutionFromTrace(audienceTrace),
		Scope:       claimResolutionFromTrace(scopeTrace),
		TenantID:    claimResolutionFromTrace(tenantTrace),
		WorkspaceID: claimResolutionFromTrace(workspaceTrace),
	}
	return auth, resolution
}

func extractClaimString(claims map[string]any, aliases []string) string {
	values := extractClaimStrings(claims, aliases)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func extractClaimStrings(claims map[string]any, aliases []string) []string {
	values, _ := extractClaimStringsWithTrace(claims, aliases)
	return values
}

func extractClaimStringsWithTrace(claims map[string]any, aliases []string) ([]string, claimExtractionTrace) {
	if len(claims) == 0 || len(aliases) == 0 {
		return nil, claimExtractionTrace{FailureReason: "claim not found"}
	}

	normalizedAliases := normalizeClaimAliases(aliases)
	if len(normalizedAliases) == 0 {
		return nil, claimExtractionTrace{FailureReason: "claim not found"}
	}

	for _, alias := range normalizedAliases {
		value, matchedPath, ok := findClaimValueByAlias(claims, alias, 0, 3, "")
		if !ok {
			continue
		}
		if out := uniqueNonEmptyStrings(claimValueToStrings(value)); len(out) > 0 {
			return out, claimExtractionTrace{
				MatchedAlias: alias,
				MatchedPath:  matchedPath,
			}
		}
	}

	return nil, claimExtractionTrace{FailureReason: "claim not found"}
}

func normalizeClaimAliases(aliases []string) []string {
	out := make([]string, 0, len(aliases))
	seen := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		normalized := normalizeClaimKey(alias)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func findClaimValueByAlias(claims map[string]any, alias string, depth, maxDepth int, pathPrefix string) (any, string, bool) {
	if depth > maxDepth {
		return nil, "", false
	}

	sortedKeys := sortedClaimKeys(claims)
	for _, key := range sortedKeys {
		value := claims[key]
		if !claimKeyMatchesAlias(key, alias) {
			continue
		}
		if len(claimValueToStrings(value)) > 0 {
			return value, joinClaimPath(pathPrefix, key), true
		}
	}

	for _, key := range sortedKeys {
		value := claims[key]
		nested, ok := value.(map[string]any)
		if !ok {
			continue
		}
		nestedPath := joinClaimPath(pathPrefix, key)
		if found, foundPath, ok := findClaimValueByAlias(nested, alias, depth+1, maxDepth, nestedPath); ok {
			return found, foundPath, true
		}
	}

	return nil, "", false
}

func sortedClaimKeys(claims map[string]any) []string {
	keys := make([]string, 0, len(claims))
	for key := range claims {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func claimKeyMatchesAlias(key string, alias string) bool {
	normalized := normalizeClaimKey(key)
	if normalized == "" {
		return false
	}

	if normalized == alias {
		return true
	}

	namespaceTail := normalizeClaimKey(lastNamespacedClaimSegment(key))
	if namespaceTail == "" || namespaceTail == normalized {
		return false
	}

	return namespaceTail == alias
}

func lastNamespacedClaimSegment(key string) string {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return ""
	}

	parts := strings.FieldsFunc(trimmed, func(r rune) bool {
		switch r {
		case '/', ':', '#', '?', '&', '=':
			return true
		default:
			return false
		}
	})
	for i := len(parts) - 1; i >= 0; i-- {
		part := strings.TrimSpace(parts[i])
		if part != "" {
			return part
		}
	}
	return ""
}

func normalizeClaimKey(key string) string {
	trimmed := strings.ToLower(strings.TrimSpace(key))
	if trimmed == "" {
		return ""
	}

	var b strings.Builder
	for _, ch := range trimmed {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func claimValueToStrings(value any) []string {
	switch typed := value.(type) {
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			return nil
		}
		return []string{text}
	case []string:
		return uniqueNonEmptyStrings(typed)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			out = append(out, claimValueToStrings(item)...)
		}
		return uniqueNonEmptyStrings(out)
	case float64:
		return []string{strconv.FormatFloat(typed, 'f', -1, 64)}
	case float32:
		return []string{strconv.FormatFloat(float64(typed), 'f', -1, 32)}
	case int:
		return []string{strconv.Itoa(typed)}
	case int64:
		return []string{strconv.FormatInt(typed, 10)}
	case int32:
		return []string{strconv.FormatInt(int64(typed), 10)}
	case uint:
		return []string{strconv.FormatUint(uint64(typed), 10)}
	case uint64:
		return []string{strconv.FormatUint(typed, 10)}
	case uint32:
		return []string{strconv.FormatUint(uint64(typed), 10)}
	case bool:
		return []string{strconv.FormatBool(typed)}
	default:
		return nil
	}
}

func uniqueNonEmptyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func claimResolutionFromTrace(trace claimExtractionTrace) claimResolution {
	return claimResolution{
		MatchedAlias:  trace.MatchedAlias,
		MatchedPath:   trace.MatchedPath,
		FailureReason: trace.FailureReason,
	}
}

func joinClaimPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}
