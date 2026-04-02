@echo off
REM ================================================
REM Kawii Daily Sync - Para Windows Task Scheduler
REM ================================================
REM Ejecuta el sync incremental de los ultimos 6 dias.
REM
REM Configurar en Task Scheduler:
REM   Programa: C:\Users\juana\Documents\analisis_datos\Proyecto_kawii\daily_sync.bat
REM   Inicio en: C:\Users\juana\Documents\analisis_datos\Proyecto_kawii
REM   Trigger: Diario a las 06:00 AM (o la hora que prefieras)
REM ================================================

cd /d "C:\Users\juana\Documents\analisis_datos\Proyecto_kawii"

REM Activar entorno virtual si existe
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

python run_daily_sync.py --days 6

REM Guardar codigo de salida
set EXIT_CODE=%ERRORLEVEL%

if %EXIT_CODE% NEQ 0 (
    echo [ERROR] Daily sync fallo con codigo %EXIT_CODE% >> daily_sync_errors.log
    echo %date% %time% - FALLO >> daily_sync_errors.log
)

exit /b %EXIT_CODE%
