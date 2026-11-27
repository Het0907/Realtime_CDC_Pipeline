[CmdletBinding()]
param(
    [switch]$SkipPipeline,
    [switch]$KeepConsumerLog,
    [switch]$ReuseServices
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
$python = if (Test-Path $venvPython) { $venvPython } else { "python" }

function Invoke-RequiredCommand {
    param(
        [string]$Command,
        [string[]]$Arguments
    )

    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE`: $Command $($Arguments -join ' ')"
    }
}

function Wait-ForConnector {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $status = Invoke-RestMethod -Uri "http://localhost:8083/connectors/inventory-connector/status" -Method Get
            if ($status.connector.state -eq "FAILED" -or ($status.tasks.Count -gt 0 -and $status.tasks[0].state -eq "FAILED")) {
                $trace = $status.tasks[0].trace
                if ([string]::IsNullOrWhiteSpace($trace)) {
                    $trace = $status.connector.state
                }
                throw "Debezium connector failed: $trace"
            }
            if ($status.connector.state -eq "RUNNING" -and $status.tasks[0].state -eq "RUNNING") {
                return
            }
        } catch {
            if ($_.Exception.Message.StartsWith("Debezium connector failed:")) {
                throw
            }
            # Debezium Connect may need a few seconds after its container starts.
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    throw "Debezium connector did not reach RUNNING state within $TimeoutSeconds seconds."
}

function Wait-ForConnectApi {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            return Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Get
        } catch {
            Start-Sleep -Seconds 2
        }
    } while ((Get-Date) -lt $deadline)

    throw "Debezium Connect API did not become available within $TimeoutSeconds seconds."
}

function Wait-ForConsumer {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$OutputLog,
        [string]$ErrorLog,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        if (Test-Path $OutputLog) {
            $output = Get-Content $OutputLog -Raw -ErrorAction SilentlyContinue
            if ($output -match "Listening on topics") {
                return
            }
        }
        if (Test-Path $ErrorLog) {
            $errors = Get-Content $ErrorLog -Raw -ErrorAction SilentlyContinue
            if ($errors -match "Unable to connect to Kafka") {
                throw "Bronze consumer failed to connect: $errors"
            }
        }
        if ($Process.HasExited) {
            throw "Bronze consumer exited before becoming ready."
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    throw "Bronze consumer did not become ready within $TimeoutSeconds seconds."
}

function Wait-ForDemoEvents {
    param(
        [System.Diagnostics.Process]$Process,
        [hashtable]$ExpectedEvents,
        [datetime]$StartedAt,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $seenFiles = @{}
    do {
        if ($Process.HasExited) {
            throw "Bronze consumer exited while capturing demo events. Check $consumerErrorLog."
        }
        foreach ($file in Get-ChildItem -Path "bronze_layer" -Filter "*.json" -Recurse) {
            if ($file.LastWriteTimeUtc -lt $StartedAt -or $seenFiles.ContainsKey($file.FullName)) {
                continue
            }
            $event = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json
            $seenFiles[$file.FullName] = $true
            $id = if ($event.operation -eq "d") { $event.before.id } else { $event.after.id }
            $ExpectedEvents.Remove("$($event.table):$($event.operation):$id")
        }
        if ($ExpectedEvents.Count -eq 0) {
            return
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)

    throw "Timed out waiting for Bronze events: $($ExpectedEvents.Keys -join ', '). Check $consumerLog."
}

if ($ReuseServices) {
    Write-Host "Using the running CDC demo services..." -ForegroundColor Cyan
} else {
    Write-Host "Starting CDC demo services..." -ForegroundColor Cyan
    Invoke-RequiredCommand "docker" @("compose", "up", "-d")
}
$connectors = Wait-ForConnectApi
if ($connectors -notcontains "inventory-connector") {
    Write-Host "Registering the inventory Debezium connector..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post `
        -ContentType "application/json" -InFile (Join-Path $projectRoot "register-postgres.json") | Out-Null
}
Wait-ForConnector

$runId = Get-Date -Format "yyyyMMddHHmmssfff"
$customerEmail = "demo.$runId@example.com"
$deletedEmail = "deleted.$runId@example.com"
$productName = "Demo Widget $runId"
$consumerLog = Join-Path $projectRoot ".demo-consumer-$runId.log"
$consumerErrorLog = Join-Path $projectRoot ".demo-consumer-$runId.err.log"

Write-Host "Starting the bronze consumer..." -ForegroundColor Cyan
$consumer = Start-Process -FilePath $python `
    -ArgumentList @("-u", "consumer.py", "--group-id", "bronze-demo") `
    -WorkingDirectory $projectRoot `
    -RedirectStandardOutput $consumerLog `
    -RedirectStandardError $consumerErrorLog `
    -WindowStyle Hidden `
    -PassThru

try {
    Wait-ForConsumer -Process $consumer -OutputLog $consumerLog -ErrorLog $consumerErrorLog

    Write-Host "Creating ecommerce CDC events..." -ForegroundColor Cyan
    $startedAt = [datetime]::UtcNow
    $psql = @("exec", "postgres", "psql", "-U", "postgres", "-d", "inventory", "-qAt", "-v", "ON_ERROR_STOP=1", "-c")
    $sql = @"
BEGIN;
INSERT INTO inventory.customers (first_name, last_name, email) VALUES ('Demo', 'Customer', '$customerEmail');
UPDATE inventory.customers SET first_name='UpdatedDemo' WHERE email='$customerEmail';
INSERT INTO inventory.products (name, description, weight) VALUES ('$productName', 'Repeatable CDC demo product', 1.5);
INSERT INTO inventory.orders (order_date, purchaser, quantity, product_id)
SELECT CURRENT_DATE, c.id, 2, p.id FROM inventory.customers c CROSS JOIN inventory.products p
WHERE c.email='$customerEmail' AND p.name='$productName' RETURNING purchaser, product_id, id;
INSERT INTO inventory.customers (first_name, last_name, email) VALUES ('Deleted', 'Demo', '$deletedEmail');
DELETE FROM inventory.customers WHERE email='$deletedEmail' RETURNING id;
COMMIT;
"@
    $ids = @(Invoke-RequiredCommand "docker" ($psql + $sql))
    if ($ids.Count -ne 2 -or $ids[0] -notmatch '^\d+\|\d+\|\d+$' -or $ids[1] -notmatch '^\d+$') {
        throw "Postgres did not return the customer, product, order, and deleted customer IDs."
    }
    $customerId, $productId, $orderId = $ids[0] -split '\|'
    $deletedId = $ids[1]
    $expectedEvents = @{
        "customers:c:$customerId" = $true
        "customers:u:$customerId" = $true
        "products:c:$productId" = $true
        "orders:c:$orderId" = $true
        "customers:c:$deletedId" = $true
        "customers:d:$deletedId" = $true
    }
    Write-Host "Waiting for all six demo events in Bronze..." -ForegroundColor Cyan
    Wait-ForDemoEvents -Process $consumer -ExpectedEvents $expectedEvents -StartedAt $startedAt
} finally {
    if ($consumer -and -not $consumer.HasExited) {
        Stop-Process -Id $consumer.Id -Force
    }
}

$eventCount = (Get-ChildItem -Path (Join-Path $projectRoot "bronze_layer") -Filter *.json -Recurse -ErrorAction SilentlyContinue | Measure-Object).Count
Write-Host "Bronze events available: $eventCount" -ForegroundColor Green

if (-not $SkipPipeline) {
    Write-Host "Running Silver, quality, dbt, and schema-test stages..." -ForegroundColor Cyan
    $previousSource = $env:CDC_BRONZE_SOURCE
    $env:CDC_BRONZE_SOURCE = "json"
    try {
        Invoke-RequiredCommand $python @("pipeline/run_pipeline.py")
    } finally {
        $env:CDC_BRONZE_SOURCE = $previousSource
    }
}

if (-not $KeepConsumerLog -and (Test-Path $consumerLog)) {
    Remove-Item $consumerLog -Force
}
if (-not $KeepConsumerLog -and (Test-Path $consumerErrorLog)) {
    Remove-Item $consumerErrorLog -Force
}

Write-Host "Demo complete." -ForegroundColor Green
Write-Host "Customer id: $customerId | Product id: $productId"
Write-Host 'For the JSON dashboard, set $env:CDC_BRONZE_SOURCE = "json"'
Write-Host "Then run: & '$python' -m streamlit run dashboard/app.py"
