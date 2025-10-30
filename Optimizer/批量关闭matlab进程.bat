@echo off
echo 正在结束所有 MATLAB.exe 进程...
taskkill /IM MATLAB.exe /F
echo 进程已结束。
pause