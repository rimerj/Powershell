[CmdletBinding()]
param (
    [Parameter(Mandatory=$false)]
    [string]$filePath
)

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

# RUN context
$AzureContext = Get-AzSubscription -SubscriptionId $connection.SubscriptionID | Out-Null
Set-AzContext -Subscription $connection.SubscriptionID | Out-Null
# Storage Context
$accKey = (Get-AzStorageAccountKey -ResourceGroupName "bhp-dp-warehouse-dv-rg" -Name "bhpdpdatalakedv")[0].Value
$context_storageAcct = New-AzStorageContext -StorageAccountName "bhpdpdatalakedv" -StorageAccountKey $accKey

# Set Parameters for Today's Run
$now = [System.DateTime]::Now
$dd = $now.toString("dd")
$MM = $now.toString("MM")
$yyyy = $now.toString("yyyy")
$yyyymmdd =  $now | Get-Date -Format "yyyyMMdd"
if ( $filePath -eq "") {
    # DERIVE Latest File in Blob Storage Location
    $location = "/bdp_output/bdp_extracts/LoomisFeeSchedule2021Extract/"
    $listOfFiles = Get-AzDataLakeGen2ChildItem -FileSystem "provider" -Path $location -FetchProperty -Context $context_storageAcct.Context
    $filteredFiles = $listOfFiles | Where-Object {$_.Path -match '^.*BHPROV_DAILY_[\d]{8}_[\d]{4}.txt$' } | % {$_.Path} | Sort-Object -Descending
    write-output $filteredFiles.gettype()
    write-output $filteredFiles
    if (-Not $filteredFiles) {
        throw "Unable to determine file to upload in latest-derivation search of $location"
    }
    $relativePath = $filteredFiles[0]
    $blob = "/$($relativePath)"
} else {
    $blob = $filePath # determine latest file to send
}

write-output "FILE SELECTED FOR UPLOAD: $blob"
# Send to client's SFTP site
$params_sftp = @{
    StorageAccountResourceGroup="bhp-dp-warehouse-dv-rg"
    StorageAccountName="bhpdpdatalakedv"
    StorageAccountContainerName="provider"
    fileToSend=$blob
    fileBusJobId="0744f26c-318c-4982-87f6-2db0be0e96d6"
}
$runBookResult = Start-AzAutomationRunbook -Name "SFTP_Upload" -Parameters $params_sftp -ResourceGroupName "bhp-dp-dv-aa" -AutomationAccountName "bhp-dp-dv-aa" -Wait
write-output $runBookResult
if ($runBookResult.StatusCode -eq "200") { # 200 ALWAYS, even when file never sent?!
    write-output "FILE SENT SUCCESFULLY; Deleting $blob..."
    $result = Remove-AzDataLakeGen2Item -FileSystem "provider" -Path $blob -Context $context_storageAcct.Context -Force
    if (-Not $result) {
        throw "Failed to delete $blob"
    }
} else {
    throw "SFTP StatusCode: $($runBookResult.StatusCode) - Skipping File Deletion"
}
# END #