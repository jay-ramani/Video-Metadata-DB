@echo off
cls
set PATH=%PATH%;"C:\Program Files\Python"
:loop_grab_metadata
IF %1=="" GOTO completed
python "G:\My Drive\Projects\Video Metadata DB\video_metadata_db.py" --percentage-completion --nomedia-create --verbose %1
SHIFT
GOTO loop_grab_metadata
:completed
pause
