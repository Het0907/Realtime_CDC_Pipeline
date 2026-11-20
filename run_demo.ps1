[CmdletBinding()]
param(
    [switch]$SkipPipeline,
    [switch]$KeepConsumerLog
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

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

Write-Host "Starting CDC demo services..." -ForegroundColor Cyan
Invoke-RequiredCommand "docker" @("compose", "up", "-d")
$connectors = Wait-ForConnectApi
if ($connectors -notcontains "inventory-connector") {
    Write-Host "Registering the inventory Debezium connector..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post `
        -ContentType "application/json" -InFile (Join-Path $projectRoot "register-postgres.json") | Out-Null
}
Wait-ForConnector

$connector = $null
try {
    $connector = Invoke-RestMethod -Uri "http://localhost:8083/connectors/inventory-connector/status" -Method Get
} catch {
    throw "Could not read the Debezium connector status."
}

if ($connector.connector.state -ne "RUNNING" -or $connector.tasks[0].state -ne "RUNNING") {
    throw "Debezium connector is not healthy: connector=$($connector.connector.state), task=$($connector.tasks[0].state)"
}

$runId = Get-Date -Format "yyyyMMddHHmmss"
$customerEmail = "demo.$runId@example.com"
$deletedEmail = "deleted.$runId@example.com"
$productName = "Demo Widget $runId"
$consumerLog = Join-Path $projectRoot ".demo-consumer-$runId.log"
$consumerErrorLog = Join-Path $projectRoot ".demo-consumer-$runId.err.log"

Write-Host "Starting the bronze consumer..." -ForegroundColor Cyan
$consumer = Start-Process -FilePath "python" `
    -ArgumentList @("-u", "consumer.py", "--group-id", "bronze-demo") `
    -WorkingDirectory $projectRoot `
    -RedirectStandardOutput $consumerLog `
    -RedirectStandardError $consumerErrorLog `
    -PassThru

try {
    Wait-ForConsumer -Process $consumer -OutputLog $consumerLog -ErrorLog $consumerErrorLog

    Write-Host "Creating ecommerce CDC events..." -ForegroundColor Cyan
    $customerId = (& docker exec postgres psql -U postgres -d inventory -qAt -c "INSERT INTO inventory.customers (first_name, last_name, email) VALUES ('Demo', 'Customer', '$customerEmail') RETURNING id;").Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($customerId)) {
        throw "Could not create the demo customer."
    }

    & docker exec postgres psql -U postgres -d inventory -c "UPDATE inventory.customers SET first_name='UpdatedDemo' WHERE id=$customerId;"
    if ($LASTEXITCODE -ne 0) { throw "Could not update the demo customer." }

    $productId = (& docker exec postgres psql -U postgres -d inventory -qAt -c "INSERT INTO inventory.products (name, description, weight) VALUES ('$productName', 'Repeatable CDC demo product', 1.5) RETURNING id;").Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($productId)) {
        throw "Could not create the demo product."
    }

    & docker exec postgres psql -U postgres -d inventory -c "INSERT INTO inventory.orders (order_date, purchaser, quantity, product_id) VALUES (CURRENT_DATE, $customerId, 2, $productId);"
    if ($LASTEXITCODE -ne 0) { throw "Could not create the demo order." }

    & docker exec postgres psql -U postgres -d inventory -c "INSERT INTO inventory.customers (first_name, last_name, email) VALUES ('Deleted', 'Demo', '$deletedEmail');"
    if ($LASTEXITCODE -ne 0) { throw "Could not create the delete-test customer." }

    & docker exec postgres psql -U postgres -d inventory -c "DELETE FROM inventory.customers WHERE email='$deletedEmail';"
    if ($LASTEXITCODE -ne 0) { throw "Could not create the delete CDC event." }

    Start-Sleep -Seconds 5
} finally {
    if ($consumer -and -not $consumer.HasExited) {
        Stop-Process -Id $consumer.Id -Force
    }
}

$eventCount = (Get-ChildItem -Path (Join-Path $projectRoot "bronze_layer") -Filter *.json -Recurse -ErrorAction SilentlyContinue | Measure-Object).Count
Write-Host "Bronze events available: $eventCount" -ForegroundColor Green

if (-not $SkipPipeline) {
    Write-Host "Running Silver, quality, dbt, and schema-test stages..." -ForegroundColor Cyan
    Invoke-RequiredCommand "python" @("pipeline/run_pipeline.py")
}

if (-not $KeepConsumerLog -and (Test-Path $consumerLog)) {
    Remove-Item $consumerLog -Force
}
if (-not $KeepConsumerLog -and (Test-Path $consumerErrorLog)) {
    Remove-Item $consumerErrorLog -Force
}

Write-Host "Demo complete." -ForegroundColor Green
Write-Host "Customer id: $customerId | Product id: $productId"
Write-Host "Open the dashboard with: streamlit run dashboard/app.py"
