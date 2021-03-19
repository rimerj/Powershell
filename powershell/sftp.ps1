# <#
# .History
#	.01	20200115	DATA-1253	Victor Baldwin	Original
# .Synopsis
#    PUT or GET file in SFTP
# .DESCRIPTION
#     This script executes a GET or a PUT on a specified pile path from/to blob storage to/from a SFTP location
#     Pre-requirements:
#                     AzModule ----> Install-Module -Name Posh-SSH
# .PARAMETERS
# -	ResourceGroupName: Resource group name where the server is being hosted
# -	ResourceName: server name that will have the action occur on it
# -	Operation: The action which will occur
# Operation parameter values:
#
# .EXAMPLE
#

# #>

    [CmdletBinding()]
param (
    [Parameter(Mandatory=$false)]
    [string]$StorageAccountResourceGroup
,	[Parameter(Mandatory=$false)]
    [string]$StorageAccountName
,	[Parameter(Mandatory=$false)]
    [string]$StorageAccountContainerName
,	[Parameter(Mandatory=$false)]
    [string]$fileToSend
,	[Parameter(Mandatory=$false)]
    [string]$fileBusJobId
)

##############         PREP          ##############

# Connect-AzAccount -Identity -Confirm:$false | Out-Null

# Get Context
Disable-AzContextAutosave –Scope Process | Out-Null

$connection = Get-AutomationConnection -Name "AzureRunAsConnection"

# Wrap authentication in retry logic for transient network failures
$logonAttempt = 0
while(!($connectionResult) -and ($logonAttempt -le 10))
{
    $LogonAttempt++
    # Logging in to Azure...
    $connectionResult = Connect-AzAccount `
                        -ServicePrincipal `
                        -Tenant $connection.TenantID `
                        -ApplicationId $connection.ApplicationID `
                        -CertificateThumbprint $connection.CertificateThumbprint

    Start-Sleep -Seconds 30
}

$AzureContext = Get-AzSubscription -SubscriptionId $connection.SubscriptionID | Out-Null

Set-AzContext -Subscription $connection.SubscriptionID | Out-Null

$params_kv = @{
    keyVaultName="kv-bhp-prd-biw-secrets"
    secretName="filebus-x-functions-key"
}
$secretValueText = Start-AzAutomationRunbook -Name "KeyVault_GetSecret" -Parameters $params_kv -ResourceGroupName "bhp-dp-dv-aa" -AutomationAccountName "bhp-dp-dv-aa" -Wait
$secretValueText = $secretValueText.toString()


$accKey = (Get-AzStorageAccountKey -ResourceGroupName $StorageAccountResourceGroup -Name $StorageAccountName)[0].Value
$context_storageAcct = New-AzStorageContext -StorageAccountName $StorageAccountName -StorageAccountKey $accKey

# Send to client's SFTP site
$fileUri = New-AzStorageBlobSASToken -Container $StorageAccountContainerName -Blob $filetoSend -StartTime (Get-Date) -ExpiryTime (Get-Date).addminutes(1) -FullUri -Permission r -Confirm:$false -Context $context_storageAcct.Context
$url = "https://ms-file-mover-functions-dev.azurewebsites.net/api/send/"+$fileBusJobId+"/file"
$body = @{
    fileUri = $fileUri
} | ConvertTo-Json
$header = @{
    "content-type" ="application/json"
    "x-functions-key"=$secretValueText
}
$result = Invoke-WebRequest -Method POST -Uri $url -Body $body -Headers $header -UseBasicParsing -Verbose
if (-Not $result.StatusCode -eq "200") {
    throw "Status Code $($result.StatusCode) - Upload Failed"
}