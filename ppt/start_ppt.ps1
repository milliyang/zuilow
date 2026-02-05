# Paper Trade Local Startup Script (PowerShell)

# Change to script directory (ppt root)
Set-Location $PSScriptRoot

# Note: Make sure you're in the correct conda environment (trade311 or sai)
# You can activate it manually: conda activate trade311

# Create runtime data directories
if (-not (Test-Path "run")) {
    New-Item -ItemType Directory -Path "run" | Out-Null
}
if (-not (Test-Path "run\db")) {
    New-Item -ItemType Directory -Path "run\db" | Out-Null
}
if (-not (Test-Path "run\logs")) {
    New-Item -ItemType Directory -Path "run\logs" | Out-Null
}
if (-not (Test-Path "run\opentimestamps")) {
    New-Item -ItemType Directory -Path "run\opentimestamps" | Out-Null
}

Write-Host "Starting Paper Trade service..." -ForegroundColor Green
Write-Host "Access URL: http://0.0.0.0:11182" -ForegroundColor Cyan
Write-Host ""

# Use a single .pycache directory under project root
$env:PYTHONPYCACHEPREFIX = "$PSScriptRoot\.pycache"
$env:DMS_BASE_URL = "http://mongkok:11183"
# If DMS returns 401, set DMS_API_KEY to match DMS server (same value as DMS's env DMS_API_KEY). Or add to .env.
$env:DMS_API_KEY = "your-shared-secret"

# Start Flask (via socketio)
python app.py
