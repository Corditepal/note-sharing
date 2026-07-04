@echo off
setlocal

echo Starting Flink in WSL Ubuntu...
wsl.exe -d Ubuntu -- bash -lc "cd /mnt/c/flink-1.19.3/bin && ./start-cluster.sh"
set "exit_code=%errorlevel%"

echo.
echo Flink exited with code %exit_code%
pause
