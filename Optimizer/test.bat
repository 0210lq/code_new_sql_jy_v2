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
echo dir: %CD%
cd ..\..
echo dir: %CD%
cd Trading
echo dir: %CD%
pause
