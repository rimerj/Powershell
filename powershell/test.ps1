$filePath=""
write-output "=== Running Loomis ACT Send"

# write-output "====== Getting Run Context"
# $AzureContext = Get-AzSubscription -SubscriptionId $connection.SubscriptionID | Out-Null
# Set-AzContext -Subscription $connection.SubscriptionID | Out-Null

write-output "====== Getting Storage Account"
$accKey = (Get-AzStorageAccountKey -ResourceGroupName "bhp-dp-warehouse-dv-rg" -Name "bhpdpdatalakedv")[0].Value
$context_storageAcct = New-AzStorageContext -StorageAccountName "bhpdpdatalakedv" -StorageAccountKey $accKey
write-output "   +++ Storage Account Retrieved"
# write-output $accKey
# write-output $context_storageAcct

# Set Parameters for Today's Run
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

write-output "   +++ Loomis ACT to send: $blob"
write-output "=== Loomis ACT Send Complete"