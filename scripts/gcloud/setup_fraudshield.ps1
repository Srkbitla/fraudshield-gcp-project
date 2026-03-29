param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,

    [string]$Region = "us-central1",

    [string]$DatasetLocation = "US",

    [string]$TopicName = "fraud-transactions"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RawBucketName = ("{0}-fraudshield-raw" -f $ProjectId).ToLowerInvariant()
$TempBucketName = ("{0}-fraudshield-temp" -f $ProjectId).ToLowerInvariant()
$DeadLetterTopic = "{0}-dlq" -f $TopicName
$SubscriptionName = "{0}-sub" -f $TopicName
$ServiceAccountName = "fraudshield-dataflow"
$ServiceAccountEmail = "{0}@{1}.iam.gserviceaccount.com" -f $ServiceAccountName, $ProjectId


function Invoke-LoggedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,

        [string[]]$Arguments = @()
    )

    Write-Host ""
    Write-Host ("==> {0} {1}" -f $FilePath, ($Arguments -join " "))
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw ("Command failed: {0} {1}" -f $FilePath, ($Arguments -join " "))
    }
}


function Test-CommandSuccess {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,

        [string[]]$Arguments = @()
    )

    & $FilePath @Arguments 1>$null 2>$null
    return $LASTEXITCODE -eq 0
}


function Ensure-Topic {
    param([string]$Name)

    if (Test-CommandSuccess "gcloud" @("pubsub", "topics", "describe", $Name, "--project", $ProjectId)) {
        return
    }

    Invoke-LoggedCommand "gcloud" @("pubsub", "topics", "create", $Name, "--project", $ProjectId)
}


function Ensure-Bucket {
    param([string]$BucketName)

    if (Test-CommandSuccess "gcloud" @("storage", "buckets", "describe", ("gs://{0}" -f $BucketName), "--project", $ProjectId)) {
        return
    }

    Invoke-LoggedCommand "gcloud" @(
        "storage",
        "buckets",
        "create",
        ("gs://{0}" -f $BucketName),
        "--project", $ProjectId,
        "--location", $Region,
        "--uniform-bucket-level-access"
    )
}


function Ensure-Dataset {
    param(
        [string]$DatasetId,
        [string]$Description
    )

    if (Test-CommandSuccess "bq" @("--location=$DatasetLocation", "show", "--format=none", ("{0}:{1}" -f $ProjectId, $DatasetId))) {
        return
    }

    Invoke-LoggedCommand "bq" @(
        "--location=$DatasetLocation",
        "mk",
        "--dataset",
        "--description", $Description,
        ("{0}:{1}" -f $ProjectId, $DatasetId)
    )
}


Invoke-LoggedCommand "gcloud" @(
    "services",
    "enable",
    "bigquery.googleapis.com",
    "composer.googleapis.com",
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "--project", $ProjectId
)

Ensure-Bucket -BucketName $RawBucketName
Ensure-Bucket -BucketName $TempBucketName

Ensure-Topic -Name $TopicName
Ensure-Topic -Name $DeadLetterTopic

$ProjectNumber = (& gcloud projects describe $ProjectId --format="value(projectNumber)" --project $ProjectId).Trim()
Invoke-LoggedCommand "gcloud" @(
    "pubsub",
    "topics",
    "add-iam-policy-binding",
    $DeadLetterTopic,
    "--project", $ProjectId,
    "--member", ("serviceAccount:service-{0}@gcp-sa-pubsub.iam.gserviceaccount.com" -f $ProjectNumber),
    "--role", "roles/pubsub.publisher"
)

if (-not (Test-CommandSuccess "gcloud" @("pubsub", "subscriptions", "describe", $SubscriptionName, "--project", $ProjectId))) {
    Invoke-LoggedCommand "gcloud" @(
        "pubsub",
        "subscriptions",
        "create",
        $SubscriptionName,
        "--project", $ProjectId,
        "--topic", $TopicName,
        "--ack-deadline", "60",
        "--dead-letter-topic", $DeadLetterTopic,
        "--max-delivery-attempts", "10",
        "--min-retry-delay", "10s",
        "--max-retry-delay", "300s"
    )
}

if (-not (Test-CommandSuccess "gcloud" @("iam", "service-accounts", "describe", $ServiceAccountEmail, "--project", $ProjectId))) {
    Invoke-LoggedCommand "gcloud" @(
        "iam",
        "service-accounts",
        "create",
        $ServiceAccountName,
        "--display-name", "FraudShield Dataflow Runner",
        "--project", $ProjectId
    )
}

foreach ($role in @(
    "roles/bigquery.dataEditor",
    "roles/dataflow.worker",
    "roles/pubsub.subscriber",
    "roles/storage.objectAdmin",
    "roles/viewer"
)) {
    Invoke-LoggedCommand "gcloud" @(
        "projects",
        "add-iam-policy-binding",
        $ProjectId,
        "--member", ("serviceAccount:{0}" -f $ServiceAccountEmail),
        "--role", $role,
        "--quiet"
    )
}

Ensure-Dataset -DatasetId "bronze" -Description "Validated payment transaction events."
Ensure-Dataset -DatasetId "silver" -Description "Curated transaction-level fraud data."
Ensure-Dataset -DatasetId "gold" -Description "Fraud analytics KPI tables."
Ensure-Dataset -DatasetId "ops" -Description "Fraud operations tables and data quality audit."

Write-Host ""
Write-Host "FraudShield core resources are ready."

