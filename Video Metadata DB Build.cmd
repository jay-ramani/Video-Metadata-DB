@echo off
cls
set PATH=%PATH%;"C:\Program Files\Python"
:loop_grab_metadata
IF %1=="" GOTO completed
python "G:\My Drive\Projects\Video Resolution\video_metadata_db.py" --percentage-completion %1
SHIFT
GOTO loop_grab_metadata
:completed
sort /R "G:\My Drive\Projects\Video Resolution\video_metadata_db.txt" /O "G:\My Drive\Projects\Video Resolution\video_metadata_db.txt"
pause