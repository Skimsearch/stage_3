param(
    [Parameter(Mandatory=$true)][string]$WorkerId,
    [string]$RunId = "A-worker-2nodes-run01",
    [switch]$Force
)

$pidFile = "logs/$RunId/pid-$WorkerId.txt"
if (-not (Test-Path $pidFile)) {
    Write-Warning "No existe $pidFile"
    exit 1
}

$targetPid = [int](Get-Content $pidFile)
if ($Force) {
    Stop-Process -Id $targetPid -Force -ErrorAction SilentlyContinue
    Write-Host "Worker '$WorkerId' (PID=$targetPid) terminado bruscamente (kill)." -ForegroundColor Yellow
} else {
    Stop-Process -Id $targetPid -ErrorAction SilentlyContinue
    Write-Host "Señal de parada enviada a '$WorkerId' (PID=$targetPid)." -ForegroundColor Green
}
Remove-Item $pidFile -ErrorAction SilentlyContinue