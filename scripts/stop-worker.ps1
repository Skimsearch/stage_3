param(
    [Parameter(Mandatory=$true)][string]$WorkerId,
    [string]$RunId = "A-worker-2nodes-run01",
    [switch]$Force
)

$procs = Get-CimInstance Win32_Process -Filter "Name = 'java.exe' AND CommandLine LIKE '%-Dworker.id=$WorkerId%'"
if (-not $procs) {
    Write-Warning "No se encontró ningún proceso activo para '$WorkerId'."
    exit 1
}

foreach ($p in $procs) {
    if ($Force) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "Worker '$WorkerId' (PID=$($p.ProcessId)) terminado bruscamente (kill)." -ForegroundColor Yellow
    } else {
        Stop-Process -Id $p.ProcessId -ErrorAction SilentlyContinue
        Write-Host "Señal de parada enviada a '$WorkerId' (PID=$($p.ProcessId))." -ForegroundColor Green
    }
}

Remove-Item "logs/$RunId/pid-$WorkerId.txt" -ErrorAction SilentlyContinue