@echo off
cls
set PATH=%PATH%;"C:\Program Files\Python"
echo %*
python "G:\My Drive\Projects\Video Resolution\video_metadata_db.py" --merge-metadata %*
pause