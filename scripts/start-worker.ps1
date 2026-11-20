param(
    [Parameter(Mandatory=$true)][string]$WorkerId,
    [Parameter(Mandatory=$true)][string]$Roles,
    [string]$RunId = "A-worker-2nodes-run01",
    [int]$HzPort = 5701,
    [string]$HzPublicAddress = "",
    [string]$HzMembers = "127.0.0.1:5701,127.0.0.1:5702",
    [int]$SearchPort = 7003,
    [int]$SearchThreads = 4,
    [string]$SearchAdvertiseHost = "127.0.0.1",
    [string]$ConsulUrl = "",
    [string]$DatalakeDir = "data/datalake",
    [string]$SharedIndexDir = "data/shared-index",
    [string]$LocalDir = "",
    [long]$LeaseTtlMs = 4000,
    [long]$LeaseRenewMs = 1500,
    [long]$ReaperIntervalMs = 500,
    [int]$IndexBatchSize = 5,
    [long]$IndexBatchFlushMs = 3000,
    [string]$SeedBooks = "",
    [switch]$Background
)

if ([string]::IsNullOrWhiteSpace($LocalDir)) {
    $LocalDir = "data/local/$WorkerId"
}

$env:RUN_ID = $RunId
$env:WORKER_ID = $WorkerId
$env:WORKER_ROLES = $Roles
$env:HZ_CLUSTER_NAME = "skimsearch"
$env:HZ_PORT = "$HzPort"
$env:HZ_PUBLIC_ADDRESS = $HzPublicAddress
$env:HZ_MEMBERS = $HzMembers
$env:SEARCH_PORT = "$SearchPort"
$env:SEARCH_THREADS = "$SearchThreads"
$env:SEARCH_ADVERTISE_HOST = $SearchAdvertiseHost
$env:CONSUL_URL = $ConsulUrl
$env:DATALAKE_DIR = $DatalakeDir
$env:SHARED_INDEX_DIR = $SharedIndexDir
$env:LOCAL_DIR = $LocalDir
$env:LEASE_TTL_MS = "$LeaseTtlMs"
$env:LEASE_RENEW_MS = "$LeaseRenewMs"
$env:REAPER_INTERVAL_MS = "$ReaperIntervalMs"
$env:INDEX_BATCH_SIZE = "$IndexBatchSize"
$env:INDEX_BATCH_FLUSH_MS = "$IndexBatchFlushMs"
$env:SEED_BOOKS = $SeedBooks

$jarPath = "worker/target/worker.jar"
if (-not (Test-Path $jarPath)) {
    throw "No se encuentra $jarPath. Ejecuta primero .\scripts\build.ps1"
}

if ($Background) {
    $proc = Start-Process -FilePath "java" -ArgumentList "-jar", $jarPath -PassThru -WindowStyle Hidden
    New-Item -ItemType Directory -Force -Path "logs/$RunId" | Out-Null
    $proc.Id | Out-File -FilePath "logs/$RunId/pid-$WorkerId.txt" -Encoding ascii
    Write-Host "Worker '$WorkerId' arrancado en segundo plano (PID=$($proc.Id), roles=$Roles, hzPort=$HzPort)" -ForegroundColor Cyan
} else {
    java -jar $jarPath
}