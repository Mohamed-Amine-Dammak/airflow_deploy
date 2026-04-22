param(
  [Parameter(Mandatory = $true)]
  [string[]]$SourcePaths,

  [string[]]$Containers = @("airflow-airflow-scheduler-1", "airflow-airflow-worker-1"),

  [string]$ContainerDir = "/opt/airflow/local_uploads/manual"
)

$ErrorActionPreference = "Stop"

foreach ($source in $SourcePaths) {
  if (-not (Test-Path -LiteralPath $source)) {
    throw "Source path not found: $source"
  }

  $item = Get-Item -LiteralPath $source
  $name = $item.Name
  $target = "$ContainerDir/$name"
  $targetDir = Split-Path -Path $target -Parent
  if ([string]::IsNullOrWhiteSpace($targetDir)) {
    $targetDir = "/opt/airflow/local_uploads/manual"
  }

  foreach ($container in $Containers) {
    Write-Host "Ensuring target dir in $container: $targetDir"
    docker exec $container sh -lc "mkdir -p '$targetDir'" | Out-Null
    Write-Host "Copying $source -> $container`:$target"
    docker cp "$source" "$container`:$target" | Out-Null
  }

  Write-Host "Synced: $source -> $target" -ForegroundColor Green
}
