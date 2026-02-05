# ZuiLow Web Service Startup Script (PowerShell, Flask, independent module)

# Change to script directory (zuilow root)
Set-Location $PSScriptRoot

# Add parent directory to PYTHONPATH so 'zuilow' can be imported as a package
$env:PYTHONPATH = "$PSScriptRoot\..;$env:PYTHONPATH"

# Activate conda environment
if (Get-Command conda -ErrorAction SilentlyContinue) {
    conda activate trade311
} else {
    Write-Host "Warning: conda command not found, trying to run Python directly" -ForegroundColor Yellow
}

# Runtime data under ./run/ (db, logs, etc.)
if (-not (Test-Path "run")) { New-Item -ItemType Directory -Path "run" | Out-Null }
if (-not (Test-Path "run\db")) { New-Item -ItemType Directory -Path "run\db" | Out-Null }
if (-not (Test-Path "run\logs")) { New-Item -ItemType Directory -Path "run\logs" | Out-Null }

Write-Host "Starting ZuiLow Web Service..." -ForegroundColor Green
Write-Host "Access URL: http://localhost:11180" -ForegroundColor Cyan
Write-Host "Futu Panel: http://localhost:11180/futu" -ForegroundColor Cyan
Write-Host "Scheduler: http://localhost:11180/scheduler" -ForegroundColor Cyan
Write-Host ""

# Use a single .pycache directory under project root
$env:PYTHONPYCACHEPREFIX = "$PSScriptRoot\.pycache"

# Start Flask
python app.py
