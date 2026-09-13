<#
.SYNOPSIS
  Task runner for the bike-share data platform (Windows equivalent of the Makefile).

.EXAMPLE
  .\make.ps1 setup                                   # build, provision, start everything, deploy flows
  .\make.ps1 backfill -City JC -Start 2025-01 -End 2025-12
  .\make.ps1 status
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet("help", "setup", "build", "up-core", "infra", "bootstrap", "up", "deploy-flows", "backfill",
                 "dbt-build", "dbt-docs", "bruin", "test", "lint", "status", "logs", "down", "destroy", "lock")]
    [string]$Task = "help",
    [ValidateSet("JC", "NYC")] [string]$City = "JC",
    [string]$Start = "2025-01",
    [string]$End = "2025-12"
)

# Docker writes progress to stderr; with "Stop", Windows PowerShell 5.1 would treat
# that as a failure. Success is decided by exit codes instead.
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot

function Invoke-Step([string]$Title, [scriptblock]$Block) {
    Write-Host "`n==> $Title" -ForegroundColor Cyan
    $global:LASTEXITCODE = 0
    & $Block
    if ($LASTEXITCODE -ne 0) {
        Write-Host "step failed: $Title (exit $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

function Ensure-Env {
    if (-not (Test-Path .env)) { Copy-Item .env.example .env; Write-Host "created .env from .env.example" }
}

function Run-Tool([string[]]$ToolArgs) { docker compose run --rm pipelines @ToolArgs }

switch ($Task) {
    "help" {
        Get-Help $PSCommandPath -Detailed | Out-String | Write-Host
        Write-Host "Tasks: setup build up-core infra bootstrap up deploy-flows backfill dbt-build dbt-docs bruin test lint status logs down destroy lock"
    }
    "build"    { Ensure-Env; Invoke-Step "Build pipelines image" { docker compose build pipelines } }
    "up-core"  { Ensure-Env; Invoke-Step "Start Postgres, MinIO, Kafka" { docker compose up -d --wait postgres minio kafka } }
    "infra" {
        Invoke-Step "Terraform init"  { docker compose run --rm terraform init -input=false }
        Invoke-Step "Terraform apply" { docker compose run --rm terraform apply -auto-approve -input=false }
    }
    "bootstrap"    { Invoke-Step "Create warehouse tables" { Run-Tool @("python", "-m", "bikeshare.warehouse.bootstrap") } }
    "up"           { Ensure-Env; Invoke-Step "Start platform services" { docker compose up -d --wait kestra kafka-ui producer consumer-lake consumer-live dashboard } }
    "deploy-flows" { Invoke-Step "Deploy Kestra flows" { Run-Tool @("python", "-m", "bikeshare.tools.kestra", "deploy") } }
    "setup" {
        foreach ($step in "build", "up-core", "infra", "bootstrap", "up", "deploy-flows") {
            & $PSCommandPath $step
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        }
        Write-Host "`nPlatform is up:" -ForegroundColor Green
        Write-Host "  Kestra     http://localhost:8080   ($((Get-Content .env | Select-String '^KESTRA_USER=') -replace 'KESTRA_USER=',''))"
        Write-Host "  Dashboard  http://localhost:8501"
        Write-Host "  MinIO      http://localhost:9001"
        Write-Host "  Kafka UI   http://localhost:8081"
        Write-Host "`nNext: .\make.ps1 backfill -City JC -Start 2025-01 -End 2025-12"
    }
    "backfill" {
        Invoke-Step "Run platform_backfill ($City $Start..$End) in Kestra" {
            Run-Tool @("python", "-m", "bikeshare.tools.kestra", "run", "platform_backfill",
                       "--input", "city=$City", "--input", "start_month=$Start", "--input", "end_month=$End", "--wait")
        }
    }
    "dbt-build" { Invoke-Step "dbt build" { Run-Tool @("sh", "-c", "cd /app/dbt && dbt seed && dbt build") } }
    "dbt-docs" {
        Write-Host "dbt docs at http://localhost:8082 (Ctrl+C to stop)"
        docker compose run --rm --service-ports pipelines sh -c "cd /app/dbt && dbt docs generate && dbt docs serve --host 0.0.0.0 --port 8082"
    }
    "bruin" { Invoke-Step "Bruin pipeline" { Run-Tool @("bash", "/app/bruin/run.sh") } }
    "lint"  { Invoke-Step "ruff" { Run-Tool @("ruff", "check", "src", "dashboard", "tests") } }
    "test" {
        & $PSCommandPath lint
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        Invoke-Step "pytest"    { Run-Tool @("pytest", "-q", "/app/tests") }
        Invoke-Step "dbt parse" { Run-Tool @("sh", "-c", "cd /app/dbt && dbt parse --no-partial-parse") }
        Invoke-Step "terraform validate" { docker compose run --rm terraform validate }
    }
    "status" { docker compose ps --format "table {{.Service}}\t{{.Status}}\t{{.Ports}}" }
    "logs"   { docker compose logs -f --tail 50 producer consumer-lake consumer-live }
    "down"   { docker compose down }
    "destroy" {
        $answer = Read-Host "This deletes ALL data volumes (warehouse, lake, Kafka, Kestra). Type 'yes' to continue"
        if ($answer -eq "yes") {
            docker compose --profile tools down -v
            Remove-Item -Recurse -Force infra/terraform/.terraform, infra/terraform/terraform.tfstate* -ErrorAction SilentlyContinue
        }
    }
    "lock" {
        Invoke-Step "Lock Python dependencies" {
            docker run --rm -v "${PWD}/docker/pipelines:/w" -w /w python:3.12-slim-bookworm sh -c `
                "pip install -q uv && uv pip compile requirements.txt --python-version 3.12 --python-platform x86_64-manylinux_2_28 -o requirements.lock"
        }
    }
}
