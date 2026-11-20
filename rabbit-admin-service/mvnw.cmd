@echo off
setlocal
REM DSY1107 - Maven Wrapper para Windows.
set "DIR=%~dp0"
set "MAVEN_CMD=%DIR%.mvn\wrapper\dists\apache-maven-3.9.11-bin\apache-maven-3.9.11\bin\mvn.cmd"
if exist "%MAVEN_CMD%" (
    call "%MAVEN_CMD%" %*
    exit /b %ERRORLEVEL%
)
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%DIR%.mvn\wrapper\maven-wrapper.ps1" %*
exit /b %ERRORLEVEL%
