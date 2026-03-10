$ErrorActionPreference = "Stop"

$AgentSourceBin = if ($env:AGENT_SOURCE_BIN) { $env:AGENT_SOURCE_BIN } else { ".\\agent.exe" }
$InstallRoot = if ($env:INSTALL_ROOT) { $env:INSTALL_ROOT } else { "C:\\Program Files\\AgentLedger" }
$ConfigDir = if ($env:CONFIG_DIR) { $env:CONFIG_DIR } else { "C:\\ProgramData\\AgentLedger" }
$QueueDir = if ($env:QUEUE_DIR) { $env:QUEUE_DIR } else { Join-Path $ConfigDir "queue" }
$ManagedConfigDir = if ($env:MANAGED_CONFIG_DIR) { $env:MANAGED_CONFIG_DIR } else { Join-Path $ConfigDir "config" }

New-Item -ItemType Directory -Force -Path $InstallRoot, $ConfigDir, $QueueDir, $ManagedConfigDir | Out-Null
Copy-Item -Path $AgentSourceBin -Destination (Join-Path $InstallRoot "agent.exe") -Force

$EnvFile = Join-Path $ConfigDir "agent-env.ps1"
@'
$env:AGENT_GATEWAY_URL = "http://127.0.0.1:8080"
$env:AGENT_RELEASE_CHANNEL = "stable"
$env:AGENT_CONFIG_DIR = "C:\ProgramData\AgentLedger\config"
$env:AGENT_QUEUE_DIR = "C:\ProgramData\AgentLedger\queue"
$env:AGENT_RELEASE_SIGNING_PUBLIC_KEY_FILE = "C:\ProgramData\AgentLedger\agent-release-public.pem"
'@ | Set-Content -Path $EnvFile -Encoding UTF8

Write-Host "Windows silent install template completed."
Write-Host "Binary: $InstallRoot\\agent.exe"
Write-Host "Env file: $EnvFile"
Write-Host "Validate with: agent.exe version; agent.exe status; agent.exe update check; agent.exe update status"
