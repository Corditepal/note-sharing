@echo off
setlocal EnableExtensions

set "LOGIN_UI=%~1"
if "%LOGIN_UI%"=="" (
    echo [ERROR] Missing Login_ui path.
    pause
    exit /b 1
)

echo ========================================
echo   Login_ui Frontend
echo ========================================
echo.
title Login_ui
set "NODEJS_DIR="
if exist "%ProgramFiles%\nodejs\npm.cmd" set "NODEJS_DIR=%ProgramFiles%\nodejs"
if not defined NODEJS_DIR if exist "%ProgramFiles(x86)%\nodejs\npm.cmd" set "NODEJS_DIR=%ProgramFiles(x86)%\nodejs"
if not defined NODEJS_DIR if exist "%LOCALAPPDATA%\Programs\nodejs\npm.cmd" set "NODEJS_DIR=%LOCALAPPDATA%\Programs\nodejs"

if not defined NODEJS_DIR (
    echo [ERROR] npm not found.
    echo        Install Node.js from https://nodejs.org/ and restart this script.
    pause
    exit /b 1
)

set "PATH=%NODEJS_DIR%;%PATH%"
echo Using Node.js from: %NODEJS_DIR%
call node -v
call npm -v
echo.

cd /d "%LOGIN_UI%"
if errorlevel 1 (
    echo [ERROR] Cannot enter Login_ui directory: %LOGIN_UI%
    pause
    exit /b 1
)

if not exist "node_modules" (
    echo node_modules not found, running npm install ...
    call npm install
    if errorlevel 1 (
        echo [ERROR] npm install failed.
        pause
        exit /b 1
    )
    echo.
)

echo Starting Login_ui on http://localhost:8082 ...
call npm run serve
set "exit_code=%errorlevel%"

echo.
echo Login_ui exited with code %exit_code%
pause
exit /b %exit_code%
