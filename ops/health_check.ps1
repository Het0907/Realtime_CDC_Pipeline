[CmdletBinding()]
param()

$ErrorActionPreference = "Continue"
$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectRoot
$failures = @()

function Check-Result {
    param([string]$Name, [bool]$Passed, [string]$Detail)
    if ($Passed) {
        Write-Host "[PASS] $Name - $Detail" -ForegroundColor Green
    } else {
        Write-Host "[FAIL] $Name - $Detail" -ForegroundColor Red
        $script:failures += $Name
    }
}

function Get-ContainerRunning {
    param([string]$Name)
    $status = docker inspect --format "{{.State.Status}}" $Name 2>$null
    return $status -eq "running"
}

Check-Result "Postgres container" (Get-ContainerRunning "postgres") "running"
if (Get-ContainerRunning "postgres") {
    docker exec postgres pg_isready -U postgres -d inventory *> $null
    Check-Result "Postgres database" ($LASTEXITCODE -eq 0) "inventory database accepts connections"
}

Check-Result "Kafka container" (Get-ContainerRunning "kafka") "running"
if (Get-ContainerRunning "kafka") {
    $topics = docker exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>$null
    Check-Result "Kafka topics" ($LASTEXITCODE -eq 0) "topic metadata is available"
}

try {
    $connectors = Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Get
    Check-Result "Debezium Connect API" $true "API is reachable"
    $status = Invoke-RestMethod -Uri "http://localhost:8083/connectors/inventory-connector/status" -Method Get
    $healthy = $status.connector.state -eq "RUNNING" -and $status.tasks[0].state -eq "RUNNING"
    Check-Result "Debezium inventory connector" $healthy "connector=$($status.connector.state), task=$($status.tasks[0].state)"
} catch {
    Check-Result "Debezium Connect API" $false $_.Exception.Message
}

$sparkRunning = Get-ContainerRunning "cdc-spark-streaming"
Check-Result "Spark streaming container" $sparkRunning "running"
Check-Result "Spark Bronze Delta" (Test-Path (Join-Path $projectRoot "spark_bronze\events\_delta_log")) "spark_bronze/events exists"
Check-Result "Silver Delta" (Test-Path (Join-Path $projectRoot "silver\customers\_delta_log")) "silver/customers exists"

try {
    $airflowHealth = Invoke-RestMethod -Uri "http://localhost:8080/health" -Method Get
    Check-Result "Airflow health" ($airflowHealth.scheduler.status -eq "healthy" -and $airflowHealth.metadatabase.status -eq "healthy") "scheduler and metadatabase are healthy"
} catch {
    Write-Host "[INFO] Airflow health - not running" -ForegroundColor Yellow
}

if ($failures.Count -gt 0) {
    Write-Host "Health check failed: $($failures -join ', ')" -ForegroundColor Red
    exit 1
}

Write-Host "Health check passed." -ForegroundColor Green
