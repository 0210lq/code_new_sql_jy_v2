@echo off

set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"
%ANACONDAPATH%\python -c "from running_main import PortfolioSaving_main;PortfolioSaving_main(is_realtime=False)"




pause
