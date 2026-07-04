@echo off
setlocal EnableExtensions

cd /d "%~dp0.."
set "PROJECT_ROOT=%CD%"
set "LOGIN_API=%PROJECT_ROOT%\Login_api"
set "LOGIN_UI=%PROJECT_ROOT%\Login_ui"
set "AI_BFF=%PROJECT_ROOT%\ai_bff"

echo ========================================
echo   FOLIO Application Startup
echo ========================================
echo.
echo Project root: %PROJECT_ROOT%
echo.
echo Note: Run infra startup script first (Starting folder)
echo       MySQL, Redis, MongoDB, Elasticsearch, MinIO, etc.
echo.

if not exist "%LOGIN_API%\gradlew.bat" (
    echo [ERROR] Login_api not found: %LOGIN_API%
    pause
    exit /b 1
)

if not exist "%LOGIN_UI%\package.json" (
    echo [ERROR] Login_ui not found: %LOGIN_UI%
    pause
    exit /b 1
)

if not exist "%AI_BFF%\app\main.py" (
    echo [ERROR] ai_bff not found: %AI_BFF%
    pause
    exit /b 1
)

if not exist "%LOGIN_UI%\node_modules" (
    echo [WARN] Login_ui\node_modules not found.
    echo        Run npm install in Login_ui before starting the frontend.
    echo.
)

if not exist "%AI_BFF%\.env" (
    if exist "%AI_BFF%\.env.example" (
        echo [INFO] Creating ai_bff\.env from .env.example ...
        copy /Y "%AI_BFF%\.env.example" "%AI_BFF%\.env" >nul
    ) else (
        echo [WARN] ai_bff\.env not found. AI features may use fallback responses.
    )
)

where python >nul 2>&1
if errorlevel 1 (
    echo [WARN] Python not found in PATH. AI BFF may fail to start.
    echo.
)

set "NODEJS_DIR="
if exist "%ProgramFiles%\nodejs\npm.cmd" set "NODEJS_DIR=%ProgramFiles%\nodejs"
if not defined NODEJS_DIR if exist "%ProgramFiles(x86)%\nodejs\npm.cmd" set "NODEJS_DIR=%ProgramFiles(x86)%\nodejs"
if not defined NODEJS_DIR if exist "%LOCALAPPDATA%\Programs\nodejs\npm.cmd" set "NODEJS_DIR=%LOCALAPPDATA%\Programs\nodejs"
if not defined NODEJS_DIR (
    echo [WARN] npm not found in default locations. Login_ui may fail to start.
    echo.
) else (
    echo [INFO] Node.js detected: %NODEJS_DIR%
    echo.
)

echo Starting applications in separate windows...
echo.

start "" cmd /k "cd /d ""%LOGIN_API%"" && title Login_api && gradlew.bat bootRun"
start "" cmd /k call "%~dp0start_login_ui.cmd" "%LOGIN_UI%"
start "" cmd /k "cd /d ""%AI_BFF%"" && title AI_BFF && python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000"

echo.
echo ========================================
echo   Applications launched
echo ========================================
echo.
echo   Backend API : http://localhost:8080
echo   Frontend UI : http://localhost:8082  ^<- open this in browser
echo   AI BFF      : http://localhost:8000
echo.
echo   User portal : http://localhost:8082/
echo   Main app    : http://localhost:8082/main
echo   Admin login : http://localhost:8082/admin/login
echo.
echo Wait for each window to finish starting before using the site.
echo.
pause
exit /b 0
