@echo off
setlocal

:: Check whether the script is running as administrator.
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo Not running as administrator, reopening with elevated privileges...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
    exit /b
)

echo Running as administrator.
echo Starting services...

:: CMD 1 - Start MongoDB
start "MongoDB" cmd /k "mongod.exe --dbpath=""C:\Program Files\MongoDB\Server\8.2\data"""

:: CMD 2 - Start Redis
start "Redis" cmd /k "redis-server.exe"

:: CMD 3 - Start MySQL
start "MySQL" cmd /k "net start mysql"

:: CMD 4 - Start Elasticsearch
start "Elasticsearch" cmd /k "elasticsearch.bat"

:: CMD 5 - Start RabbitMQ
start "RabbitMQ" cmd /k "rabbitmq-plugins enable rabbitmq_management && net stop RabbitMQ && net start RabbitMQ"

:: CMD 6 - Start Minio
start "Minio" cmd /k "minio.exe server ""C:\Program Files\minio\bin"" --console-address ""127.0.0.1:9005"" --address ""127.0.0.1:9000"""

:: CMD 7 - Start Flink cluster in WSL Ubuntu
start "Flink" cmd /k ""%~dp0start_flink_wsl.cmd""

echo All services have been started.
pause
exit /b
