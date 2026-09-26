@echo off
setlocal EnableExtensions

set "LATITUDE_PY=E:\anaconda3\envs\latitude\python.exe"
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "PROJECT_ROOT=%%~fI"

if not exist "%LATITUDE_PY%" (
    echo [ERROR] Python was not found: %LATITUDE_PY%
    pause
    exit /b 1
)

if not exist "%PROJECT_ROOT%\config\settings.py" (
    echo [ERROR] Project root was not found: %PROJECT_ROOT%
    pause
    exit /b 1
)

pushd "%PROJECT_ROOT%"
if errorlevel 1 (
    echo [ERROR] Cannot enter project root: %PROJECT_ROOT%
    pause
    exit /b 1
)

echo ============================================================
echo Futures data quality check
echo Project: %PROJECT_ROOT%
echo Python : %LATITUDE_PY%
echo ============================================================

call :run "02_Quant_Trading\a01_Data_Collection\c08_fact_futures_missing_bar.py"
if errorlevel 1 goto :failed

call :run "00_draft_collection_02\scripts\verify_futures_calendar_pipeline.py"
if errorlevel 1 goto :failed

echo.
echo [OK] Futures data quality check completed.
popd
pause
exit /b 0

:failed
echo.
echo [ERROR] The quality check stopped because a command failed.
popd
pause
exit /b 1

:run
echo.
echo [RUN] %LATITUDE_PY% %*
"%LATITUDE_PY%" %*
exit /b %errorlevel%
