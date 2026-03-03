package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const tokenDefaultRelativePath = ".agentledger/token.json"

type localToken struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	IDToken      string `json:"id_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
	ExpiresIn    int64  `json:"expires_in,omitempty"`
	ObtainedAt   string `json:"obtained_at,omitempty"`
	ExpiresAt    string `json:"expires_at,omitempty"`
}

func defaultTokenFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return tokenDefaultRelativePath
	}
	return filepath.Join(home, tokenDefaultRelativePath)
}

func loadLocalToken(path string) (*localToken, string, error) {
	resolvedPath, err := resolveTokenFilePath(path)
	if err != nil {
		return nil, "", err
	}

	content, err := os.ReadFile(resolvedPath)
	if err != nil {
		return nil, resolvedPath, err
	}

	var token localToken
	if err := json.Unmarshal(content, &token); err != nil {
		return nil, resolvedPath, fmt.Errorf("解析 token 文件失败: %w", err)
	}
	if strings.TrimSpace(token.AccessToken) == "" {
		return nil, resolvedPath, fmt.Errorf("token 文件缺少 access_token")
	}
	if strings.TrimSpace(token.TokenType) == "" {
		token.TokenType = "Bearer"
	}
	return &token, resolvedPath, nil
}

func saveLocalToken(path string, token localToken) (string, error) {
	resolvedPath, err := resolveTokenFilePath(path)
	if err != nil {
		return "", err
	}

	token.AccessToken = strings.TrimSpace(token.AccessToken)
	if token.AccessToken == "" {
		return "", fmt.Errorf("access_token 不能为空")
	}
	if strings.TrimSpace(token.TokenType) == "" {
		token.TokenType = "Bearer"
	}
	now := time.Now().UTC()
	if strings.TrimSpace(token.ObtainedAt) == "" {
		token.ObtainedAt = now.Format(time.RFC3339)
	}
	if token.ExpiresIn > 0 && strings.TrimSpace(token.ExpiresAt) == "" {
		token.ExpiresAt = now.Add(time.Duration(token.ExpiresIn) * time.Second).Format(time.RFC3339)
	}

	dir := filepath.Dir(resolvedPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("创建 token 目录失败: %w", err)
	}
	_ = os.Chmod(dir, 0o700)

	body, err := json.MarshalIndent(token, "", "  ")
	if err != nil {
		return "", fmt.Errorf("序列化 token 失败: %w", err)
	}
	body = append(body, '\n')

	tmpFile := resolvedPath + ".tmp"
	if err := os.WriteFile(tmpFile, body, 0o600); err != nil {
		return "", fmt.Errorf("写入 token 临时文件失败: %w", err)
	}
	if err := os.Rename(tmpFile, resolvedPath); err != nil {
		_ = os.Remove(tmpFile)
		return "", fmt.Errorf("保存 token 文件失败: %w", err)
	}
	_ = os.Chmod(resolvedPath, 0o600)

	return resolvedPath, nil
}

func (t localToken) AuthHeader() string {
	accessToken := strings.TrimSpace(t.AccessToken)
	if accessToken == "" {
		return ""
	}
	tokenType := strings.TrimSpace(t.TokenType)
	if tokenType == "" {
		tokenType = "Bearer"
	}
	return tokenType + " " + accessToken
}

func (t localToken) IsExpired(now time.Time) bool {
	expiresAt := strings.TrimSpace(t.ExpiresAt)
	if expiresAt != "" {
		parsed, err := time.Parse(time.RFC3339, expiresAt)
		if err == nil {
			return now.After(parsed)
		}
	}

	if t.ExpiresIn <= 0 {
		return false
	}
	obtainedAt := strings.TrimSpace(t.ObtainedAt)
	if obtainedAt == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339, obtainedAt)
	if err != nil {
		return false
	}
	return now.After(parsed.Add(time.Duration(t.ExpiresIn) * time.Second))
}

func resolveTokenFilePath(input string) (string, error) {
	target := strings.TrimSpace(input)
	if target == "" {
		target = defaultTokenFilePath()
	}

	expanded, err := expandPath(target)
	if err != nil {
		return "", fmt.Errorf("解析 token 路径失败: %w", err)
	}
	abs, err := filepath.Abs(expanded)
	if err != nil {
		return "", fmt.Errorf("解析 token 绝对路径失败: %w", err)
	}
	return abs, nil
}

func expandPath(path string) (string, error) {
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil || strings.TrimSpace(home) == "" {
			return "", fmt.Errorf("无法获取用户主目录")
		}
		if path == "~" {
			return home, nil
		}
		return filepath.Join(home, strings.TrimPrefix(path, "~/")), nil
	}
	return path, nil
}
