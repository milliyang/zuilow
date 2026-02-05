# DMS (Data Maintenance Service) Startup Script (PowerShell)
# DMS is an independent project, no dependency on sai directory

# Change to script directory (dms root)
Set-Location $PSScriptRoot

# Add current directory to PYTHONPATH for relative imports
$env:PYTHONPATH = "$PSScriptRoot;$env:PYTHONPATH"

# Activate conda environment
if (Get-Command conda -ErrorAction SilentlyContinue) {
    conda activate trade311
} else {
    Write-Host "Warning: conda command not found, trying to run Python directly" -ForegroundColor Yellow
}

Write-Host "Starting DMS (Data Maintenance Service)..." -ForegroundColor Green
Write-Host "Access URL: http://localhost:11183" -ForegroundColor Cyan
Write-Host "Web Interface: http://localhost:11183" -ForegroundColor Cyan
Write-Host "API Docs: http://localhost:11183/docs" -ForegroundColor Cyan
Write-Host ""

# Use a single .pycache directory under project root (PowerShell: $env, not export)
$env:PYTHONPYCACHEPREFIX = "$PSScriptRoot\.pycache"

# Start DMS service
python app.py
