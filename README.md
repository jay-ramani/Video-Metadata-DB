# Video Metadata DB

## What This Is
A multi-threaded Python script that queries video files for metadata and generates a Tab Separated Value database (text format). This TSV file can then be converted (to Open Document Format/Microsoft Excel) or used as-is with any spreadsheet program of choice and sorted as required. This helps identify any specifics or outliers. Personally, I use this to identify video files that can be upgraded to better formats (audio, resolution, bit-rate etc.), or just weed out redundant files.

Tip: Having video files in Matroska format helps greatly, since adding metadata like title and tags does not require re-enconding the video file, and is a breeze to query metadata. For example, converting mp4 to Matroska (a .mkv extension) can be done without re-encoding the content. All that is required is to convert the container of the content. One could use '[Video Container Convert](https://github.com/jay-ramani/video_container_convert)' to achieve this.

While the script queries the following parameters, it can be customized to report any number of parameters supported by '[ffprobe](https://www.ffmpeg.org/)'. Caveat: Higher number of parameters and the number of files queried cause the spreadsheet program to be slower. This is not an inherent flaw of the program itself, but has to do with the size of the data set.
* Width
* Height
* Duration (of video stream)
* Size (human friendly)
* Raw size (in bytes)
* Video codec name
* AV1/HEVC candidate for compression (if not already in one of these formats)
* Total number of streams
* Container name
* Number of audio channels (@Index 0)
* Audio codec name (@Index 0)
* Title
* External English subtitle availability (in srt format)
* Size of the above external English language subtitle
* External English subtitle availability for the hearing imparied (in srt format)
* Size of the above external English language subtitle for the hearing impaired
* Volume label
* Path on drive label

**Note**: Use a Python 3.12 environment or above to execute the script.

## External Tools Used
Obviously, [Python](https://www.python.org) is used to interpret the script itself. The probing code uses ('[ffprobe](https://www.ffmpeg.org/)' to query metadata.

## Where to Download the External Tools From
`ffprobe` is part of the open source ffmpeg package available from https://www.ffmpeg.org

## Pre-requisites for Use
Ensure you have these external tools installed and define the path appropriately to `ffprobe` under the respective Operating System checks in the function `get_path_probe()` in video_metadata_db.py:

```
path_ffprobe
```

For example:
```python
	if platform.system() == "Windows":
		path_ffprobe  = "C:\\ffmpeg\\bin\\ffprobe.exe"
	else:
		path_ffprobe  = "/usr/bin/ffprobe"
```
**Note**: Windows path separators have to be double escaped using another backslash, as shown in the example above. On Linux, unless these tools have already been added to the PATH environment variable, you would have to update the environment, or manually feed the path, however, it is usual for mmpeg to exist in a default installation with the path already covered.

If you'd like a tooltip notification on Windows 10 and above, install [win10toast](https://pypi.org/project/win10toast/) with `pip install win10toast`. Tooltips on Linux are supported natively in the script (thanks to `notify-send`).

## How to Batch Process/Use on Single Files
### Batch Processing Recursively/A Selection Through a Simple Right-Click
  On Windows, create a file called "Video Metadata DB Build.cmd", or whatever you like but with a .cmd extension, paste the contents as below, and on the Windows Run window, type "shell:sendto" and copy this file in the directory that opens (this is where your items that show up on right-clicking and choosing 'Send To' appear):
```batch
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
```
  Note: In the 3rd line above, ensure you set the path correctly for your Python installation, and in the 6th line, the path to where you download the Python script file to.

  Once you're done with the above, all you have to do is right-click on any directory (or even a selection of them!) containing video files, use 'Send To' to send to the command name saved above ('Video Metadata DB Build.cmd', as in the example above), and the script will recursively scan through directories and query metadata from ffprobe supported video files.
  
  I've included this .cmd file as well, so feel free to edit and set parameters according to your installation.

  Since Linux (or any Unix like OS) use varies with a lot of desktop environments, I'm not going to delve into getting verbose here; you can refer your distribution's documentation to figure it out.

### Batch Processing Recursively Through a Command
```
  python "C:\Users\<user login>\Video Metadata DB\video_metadata_db.py" --percentage-completion --nomedia-create --verbose <path to a directory containing video files> <path to another directory...> <you get the picture!>
```

## Excluding directories from being queried
Any directory with a specific name requiring to be excluded from being queried can be added to a filter list in the `process_dir()` function. Regardless of which path is recursed into, a directory that matches a filter will be skipped.

## Options
The following options are parsed currently, out of which the one for differentially updating the CSV database is work in progress

* `--percentage-completion`, or `-p`: Report the percentage of completion. This comes handy when tagging a large number of files recursively (either with the right-click 'Send To' option, or through the command line). You might want to skip this option if you'd like the script to execute faster.
* `--merge-metadata`, or `-m`: Merge multiple metadata CSV databases. This is useful when you have multiple CSV metadata files from multiple drives and/or directories and would like to have them all consolidated in a single database. A DOS script is included in the repository to additionally sort the resulting database in descreasing order of horizontal video resolution (the sort can be customised to apply to a field of your choice by modiifying the script).
* `--update-metadata-db`, or `-u`: Update the resolution statistics file with metadata for selected file(s). This is used to update (only the delta of) selected files. Currenly, **work in progress**, and is not implemented.
* `--nomedia-create`, or `-n`: Create a .nomedia file under directories to be filtered from media scrapers to assist programs like Kodi
* `--verbose`, or `-v`: Report non-error messages
* `--help`, or `-h`: Usage help for command line options

## Reporting a Summary
At the end of its execution, the script presents a summary of files probed, failures (if any) and time taken. Again, this comes in handy when dealing with a large number of files.

## Logging
For a post-mortem, or simply quenching curiosity, a log file is generated with whatever is attempted by the script. This log is generated in the local application data directory (applicable to Windows), under my name (Jay Ramani). For example, this would be `C:\Users\<user login>\AppData\Local\Jay Ramani\video_metadata_db`.

## TODO (What's Next)
A GUI front-end to make things easy

## Known Issues
* Bug with reporting the time taken to commit to the database

## Testing and Reporting Bugs
The script has been tested on Windows 10, 11 and on Manjaro Linux (XFCE). Would be great if someone can help with testing on other platforms and provide feedback.

To report bugs, use the issue tracker with GitHub.

## End User License Agreement
This software is released under the GNU General Public License version 3.0 (GPL3), and you agree to this license for any use of the software

## Disclaimer
Though not possible, since the source files are merely read and not written to, I am not responsible for any corruption of your files. Needless to say, you should always backup before trying anything on your precious data.
