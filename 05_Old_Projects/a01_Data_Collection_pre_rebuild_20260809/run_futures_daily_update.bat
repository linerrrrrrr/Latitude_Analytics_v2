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
echo Futures daily incremental update
echo Project: %PROJECT_ROOT%
echo Python : %LATITUDE_PY%
echo ============================================================

call :run "02_Quant_Trading\a01_Data_Collection\c01_dimension_trade_calendar.py"
if errorlevel 1 goto :failed

call :run "02_Quant_Trading\a01_Data_Collection\c02_dimension_futures_variety_calendar.py"
if errorlevel 1 goto :failed

echo.
echo [INFO] Updating contract calendar and JQData futures metadata.
call :run "02_Quant_Trading\a01_Data_Collection\c03_dimension_futures_contract_calendar.py"
if errorlevel 1 goto :failed

call :run "02_Quant_Trading\a01_Data_Collection\c06_dimension_futures_session_schedule_signal.py"
if errorlevel 1 goto :failed

call :run "02_Quant_Trading\a01_Data_Collection\c07_fact_futures_fetch_status.py"
if errorlevel 1 goto :failed

call :run "02_Quant_Trading\a01_Data_Collection\c04_fact_futures_daily.py"
if errorlevel 1 goto :failed

echo.
echo [OK] Futures daily incremental update completed.
popd
pause
exit /b 0

:failed
echo.
echo [ERROR] The workflow stopped because a command failed.
popd
pause
exit /b 1

:run
echo.
echo [RUN] %LATITUDE_PY% %*
"%LATITUDE_PY%" %*
exit /b %errorlevel%
