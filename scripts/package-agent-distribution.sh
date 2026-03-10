#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DIST_AGENT_DIR="${DIST_AGENT_DIR:-$ROOT_DIR/dist/agent}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/dist/agent-distribution}"
CHANNEL="${AGENT_DISTRIBUTION_CHANNEL:-stable}"
PRIVATE_KEY_FILE="${AGENT_RELEASE_SIGNING_PRIVATE_KEY_FILE:-}"
PUBLIC_KEY_FILE="${AGENT_RELEASE_SIGNING_PUBLIC_KEY_FILE:-}"
VERSION="${AGENT_DISTRIBUTION_VERSION:-$(sed -n 's/^[[:space:]]*version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$ROOT_DIR/clients/agent/main.go" | head -n 1)}"

if [[ -z "${VERSION}" ]]; then
  echo "无法解析 Agent 版本，请显式设置 AGENT_DISTRIBUTION_VERSION。" >&2
  exit 1
fi

hash_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1"
    return
  fi
  shasum -a 256 "$1"
}

hash_hex() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
    return
  fi
  shasum -a 256 "$1" | awk '{print $1}'
}

verify_or_build_agent_artifacts() {
  if [[ "${SKIP_AGENT_CROSS_BUILD:-0}" == "1" ]] && bash "$ROOT_DIR/scripts/verify-agent-cross-artifacts.sh"; then
    return
  fi
  echo "重建最新 Agent 跨平台产物..."
  bash "$ROOT_DIR/scripts/build-agent-cross.sh"
  bash "$ROOT_DIR/scripts/verify-agent-cross-artifacts.sh"
}

copy_public_key_assets() {
  mkdir -p "$OUT_DIR/public"
  cp "$ROOT_DIR/docs/templates/agent-release-signing-public.pem.example" \
    "$OUT_DIR/public/AGENT_RELEASE_SIGNING_PUBLIC_KEY.pem.example"
  if [[ -n "$PUBLIC_KEY_FILE" ]]; then
    cp "$PUBLIC_KEY_FILE" "$OUT_DIR/public/AGENT_RELEASE_SIGNING_PUBLIC_KEY.pem"
  fi
}

package_target() {
  local goos="$1"
  local goarch="$2"
  local source_name="$3"
  local binary_name="$4"
  local installer_template="$5"
  local installer_name="$6"
  local env_template="$7"

  local target_dir="$OUT_DIR/$goos/$goarch"
  local payload_dir="$target_dir/package"
  local archive_name="agent-$goos-$goarch.tar.gz"
  local archive_path="$target_dir/$archive_name"
  local manifest_path="$target_dir/release-manifest.json"
  local package_sums_path="$payload_dir/SHA256SUMS.txt"
  local target_sums_path="$target_dir/SHA256SUMS.txt"

  rm -rf "$target_dir"
  mkdir -p "$payload_dir"

  cp "$DIST_AGENT_DIR/$source_name" "$payload_dir/$binary_name"
  cp "$ROOT_DIR/docs/templates/$installer_template" "$payload_dir/$installer_name"
  cp "$ROOT_DIR/docs/templates/$env_template" "$payload_dir/.env.example"
  cp "$ROOT_DIR/docs/templates/agent-release-signing-public.pem.example" \
    "$payload_dir/AGENT_RELEASE_SIGNING_PUBLIC_KEY.pem.example"

  if [[ "$goos" != "windows" ]]; then
    chmod +x "$payload_dir/$binary_name" "$payload_dir/$installer_name"
  fi

  {
    printf '%s  %s\n' "$(hash_hex "$payload_dir/$binary_name")" "$binary_name"
    printf '%s  %s\n' "$(hash_hex "$payload_dir/$installer_name")" "$installer_name"
    printf '%s  %s\n' "$(hash_hex "$payload_dir/.env.example")" ".env.example"
    printf '%s  %s\n' "$(hash_hex "$payload_dir/AGENT_RELEASE_SIGNING_PUBLIC_KEY.pem.example")" "AGENT_RELEASE_SIGNING_PUBLIC_KEY.pem.example"
  } >"$package_sums_path"

  tar -czf "$archive_path" -C "$target_dir" package

  local install_hint
  if [[ "$goos" == "windows" ]]; then
    install_hint="tar -xzf $archive_name && pwsh -File package/$installer_name"
  else
    install_hint="tar -xzf $archive_name && bash package/$installer_name"
  fi

  local manifest_cmd=(
    bun
    ./scripts/write-agent-distribution-manifest.ts
    --archive "$archive_path"
    --output "$manifest_path"
    --os "$goos"
    --arch "$goarch"
    --version "$VERSION"
    --channel "$CHANNEL"
    --binary "package/$binary_name"
    --installer "package/$installer_name"
    --envExample "package/.env.example"
    --publicKeyExample "package/AGENT_RELEASE_SIGNING_PUBLIC_KEY.pem.example"
    --installHint "$install_hint"
  )
  if [[ -n "$PRIVATE_KEY_FILE" ]]; then
    manifest_cmd+=(--privateKeyFile "$PRIVATE_KEY_FILE")
  fi
  "${manifest_cmd[@]}"

  {
    printf '%s  %s\n' "$(hash_hex "$archive_path")" "$archive_name"
    printf '%s  %s\n' "$(hash_hex "$manifest_path")" "$(basename "$manifest_path")"
  } >"$target_sums_path"

  echo "已生成分发目录: $target_dir"
}

verify_or_build_agent_artifacts
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
copy_public_key_assets

package_target \
  "linux" \
  "amd64" \
  "agent-linux-amd64" \
  "agent" \
  "agent-silent-install-linux.sh" \
  "silent-install.sh" \
  "agent-distribution-linux.env.example"

package_target \
  "linux" \
  "arm64" \
  "agent-linux-arm64" \
  "agent" \
  "agent-silent-install-linux.sh" \
  "silent-install.sh" \
  "agent-distribution-linux.env.example"

package_target \
  "darwin" \
  "amd64" \
  "agent-darwin-amd64" \
  "agent" \
  "agent-silent-install-macos.sh" \
  "silent-install.sh" \
  "agent-distribution-macos.env.example"

package_target \
  "darwin" \
  "arm64" \
  "agent-darwin-arm64" \
  "agent" \
  "agent-silent-install-macos.sh" \
  "silent-install.sh" \
  "agent-distribution-macos.env.example"

package_target \
  "windows" \
  "amd64" \
  "agent-windows-amd64.exe" \
  "agent.exe" \
  "agent-silent-install-windows.ps1" \
  "silent-install.ps1" \
  "agent-distribution-windows.env.example"

package_target \
  "windows" \
  "arm64" \
  "agent-windows-arm64.exe" \
  "agent.exe" \
  "agent-silent-install-windows.ps1" \
  "silent-install.ps1" \
  "agent-distribution-windows.env.example"

TOP_LEVEL_SUMS="$OUT_DIR/SHA256SUMS.txt"
{
  find "$OUT_DIR" -mindepth 2 -maxdepth 4 -type f \
    \( -name 'agent-*.tar.gz' -o -name 'release-manifest.json' -o -name 'SHA256SUMS.txt' \) \
    | LC_ALL=C sort \
    | while IFS= read -r file; do
        [[ "$file" == "$TOP_LEVEL_SUMS" ]] && continue
        rel="${file#"$OUT_DIR"/}"
        printf '%s  %s\n' "$(hash_hex "$file")" "$rel"
      done
} > "$TOP_LEVEL_SUMS"

echo "Agent 分发打包完成。"
echo "输出目录: $OUT_DIR"
