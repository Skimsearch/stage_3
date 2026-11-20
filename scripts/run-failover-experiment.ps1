param(
    [string]$RunId = "A-worker-2nodes-run04",
    [int]$NumBooks = 6,
    [long]$LeaseTtlMs = 4000,
    [long]$LeaseRenewMs = 1500,
    [long]$ReaperIntervalMs = 500
)

$ErrorActionPreference = "Stop"

# 1. Limpiar procesos worker previos
Get-CimInstance Win32_Process -Filter "Name = 'java.exe' AND CommandLine LIKE '%worker.jar%'" |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

# Limpiar logs previos si se repite el mismo RunId
if (Test-Path "logs/$RunId") {
    Remove-Item -Recurse -Force "logs/$RunId"
}

$seed = (1..$NumBooks) -join ","

# 2. Arrancar w1 y w2
.\scripts\start-worker.ps1 -WorkerId w1 -Roles crawl -RunId $RunId -HzPort 5701 `
    -LeaseTtlMs $LeaseTtlMs -LeaseRenewMs $LeaseRenewMs -ReaperIntervalMs $ReaperIntervalMs -Background

.\scripts\start-worker.ps1 -WorkerId w2 -Roles crawl -RunId $RunId -HzPort 5702 `
    -LeaseTtlMs $LeaseTtlMs -LeaseRenewMs $LeaseRenewMs -ReaperIntervalMs $ReaperIntervalMs -SeedBooks $seed -Background

# 3. Esperar al instante exacto en que w2 reclama su primera tarea
$w2Log = "logs/$RunId/worker-w2.log"
$deadline = (Get-Date).AddSeconds(30)
$claimed = $false
while ((Get-Date) -lt $deadline) {
    if ((Test-Path $w2Log) -and (Select-String -Path $w2Log -Pattern "event=task-claimed" -Quiet)) {
        $claimed = $true
        break
    }
    Start-Sleep -Milliseconds 150
}

if (-not $claimed) {
    throw "w2 no reclamo ninguna tarea en 30 segundos."
}

# 4. Esperar 400ms (w2 esta a mitad de una tarea de 1500ms) y matar w2 bruscamente
Start-Sleep -Milliseconds 400
.\scripts\stop-worker.ps1 -WorkerId w2 -RunId $RunId -Force

# 5. Esperar a que w1 detecte el lease caducado, retome la tarea huerfana y complete las $NumBooks tareas
$maxWaitSec = [math]::Ceiling($LeaseTtlMs / 1000.0) + ($NumBooks * 2) + 10
$waitDeadline = (Get-Date).AddSeconds($maxWaitSec)
while ((Get-Date) -lt $waitDeadline) {
    $completed = (Select-String -Path "logs/$RunId/worker-*.log" -Pattern "event=task-completed" | Measure-Object).Count
    if ($completed -ge $NumBooks) {
        break
    }
    Start-Sleep -Milliseconds 500
}

# 6. Apagar w1 ordenadamente y generar tablas
.\scripts\stop-worker.ps1 -WorkerId w1 -RunId $RunId
Start-Sleep -Seconds 1
java scripts/worker/AnalyzeWorkerLogs.java $RunId