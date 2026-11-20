param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$MavenArgs
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$WrapperDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$MvnDir = Split-Path -Parent $WrapperDir
$ProjectDir = Split-Path -Parent $MvnDir
$PropertiesPath = Join-Path $WrapperDir 'maven-wrapper.properties'

if (-not (Test-Path $PropertiesPath)) {
    Write-Error "No se encontro $PropertiesPath"
    exit 1
}

$Properties = @{}
Get-Content $PropertiesPath | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith('#') -and $line.Contains('=')) {
        $parts = $line.Split('=', 2)
        $Properties[$parts[0].Trim()] = $parts[1].Trim()
    }
}

$DistributionUrl = $Properties['distributionUrl']
$MavenVersion = $Properties['mavenVersion']

if (-not $DistributionUrl -or -not $MavenVersion) {
    Write-Error 'maven-wrapper.properties debe contener distributionUrl y mavenVersion.'
    exit 1
}

# Comprobar Java antes de descargar Maven.
$JavaOk = $false
if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME 'bin\java.exe'))) {
    $JavaOk = $true
} elseif (Get-Command java -ErrorAction SilentlyContinue) {
    $JavaOk = $true
}

if (-not $JavaOk) {
    Write-Host 'ERROR: No se encontro Java.' -ForegroundColor Red
    Write-Host 'Instale/configure JDK 21 y verifique con: java -version'
    exit 1
}

$CacheRoot = Join-Path $WrapperDir 'dists'
$DistributionDir = Join-Path $CacheRoot ("apache-maven-{0}-bin" -f $MavenVersion)
$MavenHome = Join-Path $DistributionDir ("apache-maven-{0}" -f $MavenVersion)
$ZipPath = Join-Path $DistributionDir ("apache-maven-{0}-bin.zip" -f $MavenVersion)

if (-not (Test-Path (Join-Path $MavenHome 'bin\mvn.cmd'))) {
    New-Item -ItemType Directory -Force -Path $DistributionDir | Out-Null

    Write-Host "Maven Wrapper DSY1107: Maven $MavenVersion no esta en cache." -ForegroundColor Cyan
    Write-Host 'Descargando Maven. La primera ejecucion requiere conexion a Internet...'

    try {
        Invoke-WebRequest -Uri $DistributionUrl -OutFile $ZipPath -UseBasicParsing
    } catch {
        Write-Host 'ERROR: No fue posible descargar Maven.' -ForegroundColor Red
        Write-Host "URL: $DistributionUrl"
        Write-Host $_.Exception.Message
        exit 1
    }

    Write-Host 'Descomprimiendo Maven...'
    try {
        Expand-Archive -Path $ZipPath -DestinationPath $DistributionDir -Force
    } catch {
        Write-Host 'ERROR: No fue posible descomprimir Maven.' -ForegroundColor Red
        Write-Host $_.Exception.Message
        exit 1
    } finally {
        if (Test-Path $ZipPath) { Remove-Item $ZipPath -Force }
    }
}

$MavenCmd = Join-Path $MavenHome 'bin\mvn.cmd'
if (-not (Test-Path $MavenCmd)) {
    Write-Error "No se encontro Maven en $MavenCmd"
    exit 1
}

& $MavenCmd @MavenArgs
exit $LASTEXITCODE
