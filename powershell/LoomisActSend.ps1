[CmdletBinding()]
param (
    [Parameter(Mandatory=$false)]
    [string]$filePath
)

write-output "=== Running Loomis ACT Send"

# Get Context
Disable-AzContextAutosave –Scope Process | Out-Null
$connection = Get-AutomationConnection -Name "AzureRunAsConnection"

# Wrap authentication in retry logic for transient network failures
write-output "====== Logging into Azure"
$logonAttempt = 0
while(!($connectionResult) -and ($logonAttempt -le 10))
{
    $LogonAttempt++
    write-output "====== Login Attempt #$LogonAttempt"
    # Logging in to Azure...
    $connectionResult = Connect-AzAccount `
                -ServicePrincipal `
                -Tenant $connection.TenantID `
                -ApplicationId $connection.ApplicationID `
                -CertificateThumbprint $connection.CertificateThumbprint

    Start-Sleep -Seconds 30
}
write-output "   +++ Logged In"
write-output "====== Getting Az Context by SubscriptionID"
$AzureContext = Get-AzSubscription -SubscriptionId $connection.SubscriptionID | Out-Null
write-output "   +++ Az Context retrieved"
write-output "====== Setting Az Context"
Set-AzContext -Subscription $connection.SubscriptionID | Out-Null
write-output "   +++ Az Context set"
write-output "====== Retrieving AccKey"
$accKey = (Get-AzStorageAccountKey -ResourceGroupName "bhp-dp-warehouse-dv-rg" -Name "bhpdpdatalakedv")[0].Value
write-output "   +++ AccKey Retrieved"
write-output "====== Creating Storage Context"
$context_storageAcct = New-AzStorageContext -StorageAccountName "bhpdpdatalakedv" -StorageAccountKey $accKey
write-output "   +++ Storage Context Created"

# $now = [System.DateTime]::Now
# $dd = $now.toString("dd")
# $MM = $now.toString("MM")
# $yyyy = $now.toString("yyyy")
# $yyyymmdd =  $now | Get-Date -Format "yyyyMMdd"
# write-output "Current Date: $yyyymmdd"

if ( $filePath -eq "") {
    $location = "/bdp_output/bdp_extracts/LoomisFeeSchedule2021Extract/"
    Write-Output "====== Deriving Source File from path $location"

    # DERIVE Latest File in Blob Storage Location
    $listOfFiles = Get-AzDataLakeGen2ChildItem -FileSystem "provider" -Path $location -FetchProperty -Context $context_storageAcct.Context
    $filteredFiles = @($listOfFiles | Where-Object {$_.Path -match '^.*BHPROV_DAILY_[\d]{8}_[\d]{4}.txt$' } | % {$_.Path} | Sort-Object -Descending)
    if (-Not $filteredFiles) {
        throw "   --- Unable to determine file to upload in latest-derivation search of $location"
    }
    write-output "   +++ $($filteredFiles.Length) Files Found in $location"
    write-output $filteredFiles

    $relativePath = $filteredFiles[0]
    $blob = "/$($relativePath)"
} else {
    $blob = $filePath # determine latest file to send
}

write-output "   +++ Loomis ACT file to send: $blob"

# Send to client's SFTP site
$params_sftp = @{
    StorageAccountResourceGroup="bhp-dp-warehouse-dv-rg"
    StorageAccountName="bhpdpdatalakedv"
    StorageAccountContainerName="provider"
    fileToSend=$blob
    fileBusJobId="0744f26c-318c-4982-87f6-2db0be0e96d6"
}
write-output "====== Executing SFTP_Upload RunBook in (and waiting for completion)"
$runBookResult = Start-AzAutomationRunbook -Name "SFTP_Upload" -Parameters $params_sftp -ResourceGroupName "bhp-dp-dv-aa" -AutomationAccountName "bhp-dp-dv-aa" -Wait
write-output "   === Result:"
write-output $runBookResult

if ($runBookResult.StatusCode -eq "200") { # 200 ALWAYS, even when file never sent?!
    write-output "   +++ FILE SENT SUCCESFULLY"
    write-output "====== Deleting Source File $blob"
    $result = Remove-AzDataLakeGen2Item -FileSystem "provider" -Path $blob -Context $context_storageAcct.Context -Force
    if (-Not $result) {
        throw "   --- Failed to delete $blob"
    } else {
        Write-Output "   +++ Deleted: $blob"
    }
} else {
    throw "   --- SFTP StatusCode: $($runBookResult.StatusCode) - Skipping File Deletion"
}

write-output "=== Loomis ACT Send Complete"