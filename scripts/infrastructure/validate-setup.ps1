# Azure Setup Validation Script
param(
    [string]$ConfigFile = "../config/azure-config-dev.json"
)

if (-not (Test-Path $ConfigFile)) {
    Write-Host "Config file not found: $ConfigFile" -ForegroundColor Red
    exit 1
}

$config = Get-Content $ConfigFile | ConvertFrom-Json

Write-Host "Validating Azure Setup..." -ForegroundColor Green
Write-Host "Using config: $ConfigFile" -ForegroundColor Cyan

# Check if resources exist
$resources = @(
    @{Type="Resource Group"; Name=$config.resourceGroup; Command="az group show --name $($config.resourceGroup)"}
    @{Type="Storage Account"; Name=$config.storageAccount; Command="az storage account show --name $($config.storageAccount) --resource-group $($config.resourceGroup)"}
    @{Type="Key Vault"; Name=$config.keyVault; Command="az keyvault show --name $($config.keyVault) --resource-group $($config.resourceGroup)"}
)

$allValid = $true

foreach ($resource in $resources) {
    try {
        $result = Invoke-Expression $resource.Command 2>$null
        if ($result) {
            Write-Host "✓ $($resource.Type): $($resource.Name)" -ForegroundColor Green
        }
        else {
            Write-Host "✗ $($resource.Type): $($resource.Name) - Not found" -ForegroundColor Red
            $allValid = $false
        }
    }
    catch {
        Write-Host "✗ $($resource.Type): $($resource.Name) - Error checking" -ForegroundColor Red
        $allValid = $false
    }
}

# Check containers
Write-Host "`nChecking storage containers..." -ForegroundColor Cyan
foreach ($container in $config.containers) {
    try {
        $result = az storage container exists --account-name $config.storageAccount --name $container --auth-mode key --query "exists" -o tsv
        if ($result -eq "true") {
            Write-Host "✓ Container: $container" -ForegroundColor Green
        }
        else {
            Write-Host "✗ Container: $container - Not found" -ForegroundColor Red
            $allValid = $false
        }
    }
    catch {
        Write-Host "✗ Container: $container - Error checking" -ForegroundColor Red
        $allValid = $false
    }
}

if ($allValid) {
    Write-Host "`n✅ All resources validated successfully!" -ForegroundColor Green
    Write-Host "Azure infrastructure is ready for data vault implementation." -ForegroundColor Cyan
}
else {
    Write-Host "`n❌ Some resources failed validation. Please check your setup." -ForegroundColor Red
    exit 1
}