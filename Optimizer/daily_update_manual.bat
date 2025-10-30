@echo off
::title Optimizer_python

set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

for /f "tokens=1-3 delims=:." %%a in ("%time%") do (
    set hour=%%a
    set minute=%%b
)

setlocal enabledelayedexpansion
echo Start Time:%time%

echo Running optimizer_update...
cd /d "%SCRIPT_DIR%\Optimizer_python"
%ANACONDAPATH%\python -c "from optimizer_update_main import update_optimizer_main; update_optimizer_main()"
cd ..\..
cd Trading
%ANACONDAPATH%\python -c "from running_main import PortfolioSaving_main;PortfolioSaving_main(is_realtime=False)"
%ANACONDAPATH%\python -c "from running_main import TradingOder_main;TradingOder_main(is_realtime=False)"


pause
