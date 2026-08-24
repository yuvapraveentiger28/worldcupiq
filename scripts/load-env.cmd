@echo off
REM Loads .env into the current cmd.exe session's environment variables.
REM Usage:  scripts\load-env.cmd     (run directly at the prompt; if calling this from
REM                                   another .bat/.cmd file, use `call scripts\load-env.cmd`
REM                                   so control returns to the caller afterward)

set "ENVFILE=%~dp0..\.env"

if not exist "%ENVFILE%" (
    echo .env not found at %ENVFILE% - copy .env.example to .env and fill in DATABRICKS_HOST / DATABRICKS_TOKEN first.
    exit /b 1
)

for /f "usebackq eol=# tokens=1,* delims==" %%A in ("%ENVFILE%") do (
    if not "%%A"=="" set "%%A=%%B"
)

if "%DATABRICKS_TOKEN%"=="" echo WARNING: DATABRICKS_TOKEN is empty - fill in .env before running databricks bundle commands.
if not "%DATABRICKS_TOKEN%"=="" echo Loaded DATABRICKS_HOST and DATABRICKS_TOKEN from .env into this session.
