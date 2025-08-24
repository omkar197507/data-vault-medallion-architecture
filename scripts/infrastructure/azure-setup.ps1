# Azure Infrastructure Setup Script
param(
    [string]$ResourceGroupName = "data-vault-medallion-rg",
    [string]$Location = "eastus",
    [string]$Environment = "dev"
)

Write-Host "==============================================" -ForegroundColor Green
Write-Host "Starting Azure Infrastructure Setup" -ForegroundColor Green
Write-Host "Environment: $Environment" -ForegroundColor Yellow
Write-Host "Location: $Location" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Green

# Generate unique names
$timestamp = Get-Date -Format "yyyyMMddHHmmss"
$storageAccountName = "dvstorage$($Environment)$($timestamp.Substring($timestamp.Length-6))".ToLower()
$databricksName = "dv-workspace-$Environment-$($timestamp.Substring($timestamp.Length-6))".ToLower()
$keyVaultName = "dv-kv-$Environment-$($timestamp.Substring($timestamp.Length-6))".ToLower()

Write-Host "Generated resource names:" -ForegroundColor Cyan
Write-Host "Storage Account: $storageAccountName" -ForegroundColor White
Write-Host "Databricks Workspace: $databricksName" -ForegroundColor White
Write-Host "Key Vault: $keyVaultName" -ForegroundColor White

# Check Azure login
try {
    $context = az account show 2>$null
    if (-not $context) {
        Write-Host "Please log in to Azure..." -ForegroundColor Yellow
        az login
    }
}
catch {
    Write-Host "Azure CLI not available. Please install Azure CLI first." -ForegroundColor Red
    exit 1
}

# Create Resource Group
Write-Host "Creating Resource Group: $ResourceGroupName" -ForegroundColor Cyan
az group create --name $ResourceGroupName --location $Location

# Create Storage Account (ADLS Gen2)
Write-Host "Creating Storage Account: $storageAccountName" -ForegroundColor Cyan
az storage account create `
    --name $storageAccountName `
    --resource-group $ResourceGroupName `
    --location $Location `
    --sku Standard_LRS `
    --kind StorageV2 `
    --enable-hierarchical-namespace true

# Create Databricks Workspace
Write-Host "Creating Databricks Workspace: $databricksName" -ForegroundColor Cyan
az databricks workspace create `
    --name $databricksName `
    --resource-group $ResourceGroupName `
    --location $Location `
    --sku premium

# Create Key Vault
Write-Host "Creating Key Vault: $keyVaultName" -ForegroundColor Cyan
az keyvault create `
    --name $keyVaultName `
    --resource-group $ResourceGroupName `
    --location $Location `
    --enable-rbac-authorization true

# Create containers in storage account
Write-Host "Creating storage containers..." -ForegroundColor Cyan
$storageKey = az storage account keys list --account-name $storageAccountName --resource-group $ResourceGroupName --query "[0].value" -o tsv

$containers = @("bronze", "silver", "gold", "sample-data")
foreach ($container in $containers) {
    az storage container create `
        --name $container `
        --account-name $storageAccountName `
        --account-key $storageKey `
        --auth-mode key
    Write-Host "Created container: $container" -ForegroundColor Green
}

# Output summary
Write-Host "==============================================" -ForegroundColor Green
Write-Host "Azure Infrastructure Setup Complete!" -ForegroundColor Green
Write-Host "==============================================" -ForegroundColor Green
Write-Host "Resource Group: $ResourceGroupName" -ForegroundColor White
Write-Host "Storage Account: $storageAccountName" -ForegroundColor White
Write-Host "Databricks Workspace: $databricksName" -ForegroundColor White
Write-Host "Key Vault: $keyVaultName" -ForegroundColor White
Write-Host "Containers: $($containers -join ', ')" -ForegroundColor White
Write-Host "Location: $Location" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Green

# Save configuration
$config = @{
    resourceGroup = $ResourceGroupName
    storageAccount = $storageAccountName
    databricksWorkspace = $databricksName
    keyVault = $keyVaultName
    location = $Location
    environment = $Environment
    timestamp = $timestamp
    containers = $containers
}

$config | ConvertTo-Json | Out-File -FilePath "../config/azure-config-$Environment.json"
Write-Host "Configuration saved to: ../config/azure-config-$Environment.json" -ForegroundColor Cyan