@echo off
title stock_app :5088
rem 后台启动 stock_app(:5088) 并把控制台输出落盘。
rem 与 start.bat 的区别: 不做依赖检查、不 pause、输出重定向到日志文件 —— 适合"后台拉起"。
rem 用法: cmd /c start D:\mylog\stock_app\start_bg.bat   (路径无空格, 无需引号)
cd /d %~dp0
echo [start_bg] %date% %time% 启动 stock_app ...
"%~dp0venv\Scripts\python.exe" main.py >> "%~dp0stock_app_console.log" 2>&1
echo [start_bg] %date% %time% 进程退出 (exitcode=%errorlevel%)
