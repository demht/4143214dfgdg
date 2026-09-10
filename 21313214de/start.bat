@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" bot.py %*
) else (
    py bot.py %*
)
if errorlevel 1 pause
