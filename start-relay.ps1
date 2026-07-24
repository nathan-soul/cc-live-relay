param(
    [int]$Port = 8765,
    [string]$ListenHost = "0.0.0.0"
)

$RelayDir = $PSScriptRoot

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    Write-Host "Python niet gevonden. Installeer Python 3.8+"
    exit 1
}

Write-Host "Dependencies checken..."
pip install -q -r "$RelayDir\requirements.txt" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "pip install mislukt"
    exit 1
}

Write-Host "Relay starten op $ListenHost`:$Port ..."
Set-Location $RelayDir
$env:HOST = $ListenHost
$env:PORT = $Port
python server.py
