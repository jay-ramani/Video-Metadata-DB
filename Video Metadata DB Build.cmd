@echo off
cls
set PATH=%PATH%;"C:\Program Files\Python"
:loop_grab_resolutions
IF %1=="" GOTO completed
python "G:\My Drive\Projects\Video Resolution\video_resolution.py" --percentage-completion --nomedia-create --verbose %1
SHIFT
GOTO loop_grab_resolutions
:completed
sort /R "G:\My Drive\Projects\Video Resolution\video_resolution.txt" /O "G:\My Drive\Projects\Video Resolution\video_resolution.txt"
pause
