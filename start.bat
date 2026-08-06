@echo off
chcp 65001 >nul
title 天勤行情系统
cd /d %~dp0

set VENV_PYTHON=%~dp0venv\Scripts\python.exe

echo ==========================================
echo   天勤 TqSdk 行情系统 - 启动中...
echo ==========================================
echo.

echo [1/2] 检查依赖...
if exist "%VENV_PYTHON%" (
  echo   使用虚拟环境 Python 3.10...
  echo   检查核心依赖...
  "%VENV_PYTHON%" -c "import tqsdk,flask,pandas" >nul 2>&1
  if %errorlevel% neq 0 (
    echo   安装核心依赖...
    "%VENV_PYTHON%" -m pip install tqsdk flask pandas -i https://pypi.doubanio.com/simple --trusted-host pypi.doubanio.com --timeout 120 --retries 2
  )
  echo   核心依赖检查完成!

  echo   检查 akshare ^(历史K线, 可选^)...
  "%VENV_PYTHON%" -c "import akshare" >nul 2>&1
  if %errorlevel% equ 0 (
    echo   akshare 已安装, 历史K线可用
  ) else (
    echo   akshare 未安装, 历史K线功能不可用 ^(不影响主力能^)
  )
) else (
  echo   虚拟环境未配置, 使用系统Python...
  python -c "import tqsdk,flask,pandas" >nul 2>&1
  if %errorlevel% neq 0 (
    python -m pip install tqsdk flask pandas -i https://pypi.doubanio.com/simple --trusted-host pypi.doubanio.com --timeout 120 --retries 2
  )
  echo   依赖检查完成!
)

echo.
echo [2/2] 启动行情服务...
echo   浏览器打开: http://127.0.0.1:5088
echo   按 Ctrl+C 停止服务
echo ==========================================
echo.

if exist "%VENV_PYTHON%" (
  "%VENV_PYTHON%" main.py
) else (
  python main.py
)

pause
