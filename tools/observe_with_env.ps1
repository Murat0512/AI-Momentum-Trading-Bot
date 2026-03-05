param(
    [string]$Date = "2026-03-04"
)

$ErrorActionPreference = "Stop"

Set-Location "$PSScriptRoot\.."

$python = "C:/Users/user/AppData/Local/Programs/Python/Python314/python.exe"
$envFile = ".env"

if (-not (Test-Path $envFile)) {
    throw ".env file not found at $(Get-Location)"
}

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith('#')) { return }

    if ($line -match '^export\s+') {
        $line = $line -replace '^export\s+', ''
    }
    if ($line -match '^set\s+') {
        $line = $line -replace '^set\s+', ''
    }

    if ($line -match '^(?<k>[A-Za-z_][A-Za-z0-9_]*)=(?<v>.*)$') {
        $k = $matches['k']
        $v = $matches['v'].Trim()
        if (($v.StartsWith('"') -and $v.EndsWith('"')) -or ($v.StartsWith("'") -and $v.EndsWith("'"))) {
            $v = $v.Substring(1, $v.Length - 2)
        }
        Set-Item -Path ("Env:" + $k) -Value $v
    }
}

& $python main.py --scan-only --debug
& $python main.py --once --debug
& $python tools/replay_report.py --date $Date --csv-out "logs/replay_report_$Date.csv"

Write-Host "Observation automation complete for $Date"
