@reg add HKCU\Console /v QuickEdit /d 0 /f
start "xxx" "E:\GO\src\github.com\chess\server_center\server_center.exe"
@ping 127.0.0.1 -n 12 >nul
@reg add HKCU\Console /v QuickEdit /d 1 /f
pause > nul

