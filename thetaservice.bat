start %USERPROFILE%\spot-fixings\runthetadata.bat
start %USERPROFILE%\spot-fixings\runthetadata3.bat
start %USERPROFILE%\spot-fixings\service.bat
rem set browser_value=chrome
set browser_value=firefox
start %browser_value% "http:\\localhost:5000"