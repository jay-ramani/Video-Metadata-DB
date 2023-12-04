# -------------------------------------------------------------------------------
# Name        : Video Metadata DB
# Purpose     : Build a Comma Separated Value (CSV) database of video and audio
#               metadata with ffprobe
#             : Note: Requires a path to where ffprobe is installed. Obviously, the
#             : OS specific ffmpeg package has to be installed as a pre-requisite.
# Author      : Jayendran Jayamkondam Ramani
# Created     : 3:56 PM + 5:30 IST 14 November 2015
# Copyright   : (c) Jayendran Jayamkondam Ramani
# Licence     : GPL v3
# Dependencies: Requires the following packages
#                   - win32api package (pip install pypiwin32)
#                   - win10toast (pip install win10toast; for Windows 10 toast notifications)
# -------------------------------------------------------------------------------

import mmap
import os
import platform
import multiprocessing
import io
import subprocess
import sys
import time
import argparse
import logging
import shutil
import itertools
import math

from pathlib import Path
# For tooltip notification on Windows
from win10toast import ToastNotifier
# For spawning threads for the I/O bound tagger
from multiprocessing.dummy import Pool as ThreadPool
from threading import Thread, Lock

# Spawn four threads for each CPU core found
COUNT_THREADS = multiprocessing.cpu_count() * 4

mutex_count = Lock()
mutex_time = Lock()
mutex_console = Lock()
mutex_file = Lock()
mutex_list_files_failed_probe = Lock()


def is_supported_platform():
	return platform.system() == "Windows" or platform.system() == "Linux"


# Show tool tip/notification/toast message
def show_toast(tooltip_title, tooltip_message):
	# Handle tool tip notification (Linux)/balloon tip (Windows; only OS v10 supported for now)
	tooltip_message = os.path.basename(__file__) + ": " + tooltip_message

	if platform.system() == "Linux":
		os.system("notify-send \"" + tooltip_title + "\" \"" + tooltip_message + "\"")
	else:
		toaster = ToastNotifier()
		toaster.show_toast(tooltip_title, tooltip_message, icon_path = None, duration = 5)


# Convert the time in nanoseconds passed in, and return hours, minutes and seconds as a string
def total_time_in_hms_get(total_time_ns):
	seconds_raw = total_time_ns / 1000000000
	seconds = round(seconds_raw)
	hours = minutes = 0

	if seconds >= 60:
		minutes = round(seconds / 60)
		seconds = seconds % 60

	if minutes >= 60:
		hours = round(minutes / 60)
		minutes = minutes % 60

	# If the quantum is less than a second, we need show a better resolution. A fractional report matters only when
	# it's less than 1.
	if (not (hours and minutes)) and (seconds_raw < 1 and seconds_raw > 0):
		# Round off to two decimals
		seconds = round(seconds_raw, 2)
	elif (not (hours and minutes)) and (seconds_raw < 60 and seconds_raw > 1):
		# Round off to the nearest integer, if the quantum is less than a minute. A fractional report doesn't matter
		# when it's more than 1.
		seconds = round(seconds_raw)

	return (str(hours) + " hour(s) " if hours else "") + (str(minutes) + " minute(s) " if minutes else "") + (str(
		seconds) + " second(s)")


# Open a file and log what we do
def logging_initialize(root):
	from appdirs import AppDirs

	# Use realpath instead to get through symlinks
	name_script_executable = os.path.basename(os.path.realpath(__file__)).partition(".")[0]
	dirs = AppDirs(name_script_executable, "Jay Ramani")

	try:
		os.makedirs(dirs.user_log_dir, exist_ok = True)
	except PermissionError:
		print("\aNo permission to write log files at \'" + dirs.user_log_dir + "\'!")
	except:
		print("\aUndefined exception!")
		print("Error", sys.exc_info())
	else:
		print("Check logging results at \'" + dirs.user_log_dir + "\'\n")

		# All good. Proceed with logging.
		logging.basicConfig(filename = dirs.user_log_dir + os.path.sep + name_script_executable + " - " +
		                               time.strftime("%Y%m%d%I%M%S%z") + '.log', level = logging.INFO,
		                    format = "%(message)s")
		logging.info("Log beginning at " + time.strftime("%d %b %Y (%a) %I:%M:%S %p %Z (GMT%z)") + " with PID: " + str(
			os.getpid()) + ", started with arguments " + str(sys.argv) + "\n")


def get_path_probe():
	if platform.system() == "Windows":
		return "C:\\ffmpeg\\bin\\ffprobe.exe"
	else:
		# If the binary is installed to the appropriate bin directories
		# (/usr/bin or /bin or /usr/local/bin), and the path configured
		# typically, this would be a cinch to execute under one of the
		# Unices.
		return "ffprobe"


# Returns the label for a drive/partition/volume. Used to
# easily locate videos on a particular disk/partition/volume
# in the report.
def get_volume_label(path):
	label = ""

	if platform.system() == "Windows":
		# We're on Windows
		drive, _ = os.path.splitdrive(path)

		if drive:
			# Import only when required
			import win32api

			label = (win32api.GetVolumeInformation(drive + os.sep))[0]
	else:
		# We're on one of the Unices. Import only when required.
		import psutil

		label = psutil.disk_partitions()[0].mountpoint

	return label


def sizeof_fmt(num, suffix = 'B'):
	for unit in ('', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi'):
		if abs(num) < 1024.0:
			return "%3.1f%s%s" % (num, unit, suffix)
		num /= 1024.0

	return "%.1f%s%s" % (num, 'Yi', suffix)


# Lock the console and log for exclusive access to print. The caller is to pass True for logging this to the error
# stream and flushing prints.
def lock_console_print_and_log(string, stream_error = False):
	with mutex_console:
		if not stream_error:
			print(string)
			logging.info(string)
		else:
			print(string, flush = True)
			logging.error(string)


# Writes video information to the stream passed, with values, tab separated
# - Tab Separated Values: TSV, like in Comma Separated Values (CSV) format.
# This is to help analysis with spreadsheet programs or parsing externally
# in other ways.
def save_video_information(file_stream, file_video, output_video, output_audio, label_volume):
	# Indices to get to the right line in the video output
	INDEX_OUTPUT_LINE_VIDEO_CODEC_LONG_NAME = 0
	INDEX_OUTPUT_LINE_VIDEO_WIDTH = 1
	INDEX_OUTPUT_LINE_VIDEO_HEIGHT = 2
	INDEX_OUTPUT_LINE_STREAMS_TOTAL = 3
	INDEX_OUTPUT_LINE_VIDEO_FORMAT_CONTAINER = 4
	INDEX_OUTPUT_LINE_TITLE = 5

	# Indices to get to the right line in the audio output
	INDEX_OUTPUT_LINE_AUDIO_CODEC_LONG_NAME = 0
	INDEX_OUTPUT_LINE_AUDIO_CHANNELS = 1

	# Split each line in the video output into a tuple element
	lines_video = (output_video.encode("utf-8")).splitlines()

	# The length of the video output has to be at least the last enumeration defined above
	# (INDEX_OUTPUT_LINE_TITLE long) and ideally INDEX_OUTPUT_LINE_TITLE + 1 long.
	# The title is discounted as not all video file have it tagged within. Any length
	# lower than INDEX_OUTPUT_LINE_TITLE indicates something is fishy.
	if len(lines_video) >= INDEX_OUTPUT_LINE_TITLE:
		# Check if we have non-zero values for the video dimensions
		if len(lines_video[INDEX_OUTPUT_LINE_VIDEO_WIDTH]) and len(lines_video[INDEX_OUTPUT_LINE_VIDEO_HEIGHT]):
			# Write video stream resolution (width and height) information
			file_stream.write("{:>4}".format(lines_video[INDEX_OUTPUT_LINE_VIDEO_WIDTH].decode("utf-8")) + "\t")
			file_stream.write("{:>4}".format(lines_video[INDEX_OUTPUT_LINE_VIDEO_HEIGHT].decode("utf-8")) + "\t")
		else:
			if len(lines_video[INDEX_OUTPUT_LINE_VIDEO_WIDTH]) == 0:
				# No resolution information available, record zeroes for the width.
				# This is to ensure we have some information in place, should we decide
				# to sort the output.
				file_stream.write("{:>04}".format("") + "\t")

			if len(lines_video[INDEX_OUTPUT_LINE_VIDEO_HEIGHT]) == 0:
				# No resolution information available, record zeroes for the height.
				# This is to ensure we have some information in place, should we decide
				# to sort the output.
				file_stream.write("{:>04}".format("") + "\t")

		# Followed by the file's size
		stat = os.stat(file_video)
		# file_stream.write("{:>10}".format(sizeof_fmt(stat.st_size)) + "\t")
		file_stream.write(sizeof_fmt(stat.st_size) + "\t")

		# Followed by the file's raw size in bytes (used for accounting by another script)
		# file_stream.write("{:>11}".format(stat.st_size) + "\t")
		file_stream.write(str(stat.st_size) + "\t")

		# Followed by the full codec name
		# file_stream.write("{:<50}".format(lines_video[INDEX_OUTPUT_LINE_VIDEO_CODEC_LONG_NAME].decode("utf-8")) + "\t")
		file_stream.write(lines_video[INDEX_OUTPUT_LINE_VIDEO_CODEC_LONG_NAME].decode("utf-8") + "\t")

		# Followed by the total number of streams [video, audio and subtitles (and possibly
		# anything else!)]
		# file_stream.write("{:>3}".format(lines_video[INDEX_OUTPUT_LINE_STREAMS_TOTAL].decode("utf-8")) + "\t")
		file_stream.write(lines_video[INDEX_OUTPUT_LINE_STREAMS_TOTAL].decode("utf-8") + "\t")

		# Followed by the container's name
		# file_stream.write("{:<35}".format(lines_video[INDEX_OUTPUT_LINE_VIDEO_FORMAT_CONTAINER].decode("utf-8")) + "\t")
		file_stream.write(lines_video[INDEX_OUTPUT_LINE_VIDEO_FORMAT_CONTAINER].decode("utf-8") + "\t")

		## Split each line in the audio output into a tuple element
		lines_audio = output_audio.splitlines()

		# Log details only if an audio stream was found at index zero
		if len(lines_audio) == INDEX_OUTPUT_LINE_AUDIO_CHANNELS + 1:
			# Write the number of channels in the stream pointed to by index zero
			# file_stream.write("{:>1}".format(lines_audio[INDEX_OUTPUT_LINE_AUDIO_CHANNELS]) + "\t")
			file_stream.write(lines_audio[INDEX_OUTPUT_LINE_AUDIO_CHANNELS] + "\t")

			# Followed by the full audio codec name
			# file_stream.write("{:<50}".format(lines_audio[INDEX_OUTPUT_LINE_AUDIO_CODEC_LONG_NAME]) + "\t")
			file_stream.write(lines_audio[INDEX_OUTPUT_LINE_AUDIO_CODEC_LONG_NAME] + "\t")
		else:
			lock_console_print_and_log(
				"No audio stream found in index zero for \'" + file_video +
				"\'. You might want to check if there is no audio available, or other audio streams.", True)

		# Followed by the file's title currently set in it's metadata. A video file
		# may not necessarily have it's title tag set; in this case, ensure we
		# accommodate substitution with a marker/blank string so that we don't use an
		# invalid index into the list.
		if len(lines_video) == INDEX_OUTPUT_LINE_TITLE:
			# file_stream.write("{:<255}".format("<Title Not Set>") + "\t")
			file_stream.write("<Title Not Set>" + "\t")
		else:
			# file_stream.write("{:<255}".format(lines_video[INDEX_OUTPUT_LINE_TITLE].decode("utf-8")) + "\t")
			file_stream.write(lines_video[INDEX_OUTPUT_LINE_TITLE].decode("utf-8") + "\t")

		# Put in a field to convey if an external subtitle file exists for the video in question
		if os.path.exists(file_video.rpartition(os.extsep)[0] + ".en.srt") or os.path.exists(
			file_video.rpartition(os.extsep)[0] + ".srt"):
			file_stream.write("Y" + "\t")
		else:
			file_stream.write("N" + "\t")

		# Followed by the parent volume label of the file.
		# Left justify by 32 characters, to comply with the maximum
		# length of a volume's label.
		# file_stream.write("{:<32}".format(label_volume) + "\t")
		file_stream.write(label_volume + "\t")

		# Followed by the file's path, after converting text to utf-8. This conversion
		# is required to handle paths or file names in non-ASCII character sets.
		file_video.encode("utf-8")

		# Strip the drive label if we're on Windows. A drive letter preceding the path
		# is useless as the previous field already states the drive/volume label.
		if platform.system() == "Windows":
			_, file_video = os.path.splitdrive(file_video)

		file_stream.write(file_video)
		file_stream.write("\n")
	else:
		lock_console_print_and_log("The number lines in the output for \'" + file_video + "\' were only " + str(
			len(lines_video)) + "! This should be at least " + str(
			INDEX_OUTPUT_LINE_TITLE) + " lines long, and ideally " + str(
			INDEX_OUTPUT_LINE_TITLE + 1) + ". Did you pass a .mkv/.mp4 file with audio only stream(s)?",
		                           True)


# Print a spacer after every file's processing for sifting through the output
# and log
# def print_and_log_spacer(count, path_file):
#	print(f"----- File {count} -----\n")
#	print("\'" + path_file + "\'\n")
#	print(f"----- File {count} processing complete -----\n\n")
#	logging.info(f"----- File {count} -----\n")
#	logging.info("\'" + path_file + "\'\n")
#	logging.info(f"----- File {count} processing complete -----\n\n")


def query_file_update_check(path_file, file_dimensions):
	update = True

	# As we've been asked to only update the resolution statistics, instead
	# of refreshing the whole file, rewind the offset to the beginning of the
	# statistics db, check if the file in question already exists in our db.
	# If so, ignore the request, else, move to the end of the file and position
	# the offset for the update.

	# Backup the current position of the file pointer
	offset_db_original = file_dimensions.tell()

	# Head to the beginning of the db file
	file_dimensions.seek(0, 0)
	content_db = mmap.mmap(file_dimensions.fileno(), 0, access = mmap.ACCESS_READ)

	if content_db.find(os.path.basename(os.path.dirname(path_file))) == -1:
		# Couldn't find the file in the db, so there would be no redundancy in
		# adding this entry. Restore the offset to the end of the file.
		file_dimensions.seek(0, offset_db_original)
	else:
		# The file to be probed already exists in the db. Hence, set a flag for the caller
		# ignore and return.
		update = False

	return update


# Print completion status at every checkpoint defined by CHECKPOINT_FILES_QUERIED
def percentage_completion_print(count_processed, count_total):
	# Default CHECKPOINT_FILES_QUERIED to 1% of the files to be processed
	CHECKPOINT_FILES_QUERIED = round(query_file.total_count_percentage * (1 / 100))

	# If count_total is too low, set CHECKPOINT_FILES_QUERIED accordingly for
	# printing progress after every file. This is to prevent a divide-by-zero
	# error during printing progress down below if CHECKPOINT_FILES_QUERIED is
	# zero.
	if not CHECKPOINT_FILES_QUERIED:
		CHECKPOINT_FILES_QUERIED = 1

	# For every checkpoint defined in CHECKPOINT_FILES_QUERIED, print percentage completion
	if not (count_processed % CHECKPOINT_FILES_QUERIED):
		if query_file.total_count_queried < query_file.total_count_percentage:
			percent_complete = round((query_file.total_count_queried / query_file.total_count_percentage) * 100)

			# When we're close to the total count, round() would ceil the processed count to
			# 100, which is not accurate. Hence, stall the bump to 100.
			if percent_complete == 100:
				percent_complete -= 1
			# percent_complete = math.floor((query_file.total_count_queried / query_file.total_count_percentage) * 100)

			# percent_complete_str = str(percent_complete)
			percent_complete_str = str(
				math.floor((query_file.total_count_queried / query_file.total_count_percentage) * 100))

			# For a large count of total number of files, the percentage of processed files
			# would initially be zero. There's no point reporting a zero percentage completion;
			# hence, skip until we reach at least one percent.
			# if percent_complete:
			print("\n-----------------------------")
			logging.info("-----------------------------")

			print(percent_complete_str + "% of files in queue queried")
			logging.info(percent_complete_str + "% of files in queue queried")

			print("-----------------------------\n\n")
			logging.info("-----------------------------\n")
	else:
		if query_file.total_count_queried == query_file.total_count_percentage:
			print("\nAll files in queue queried\n")
			logging.info("\nAll files in queue queried\n")


# Generate a name for the database we're going to build using the drive label of the file(s) getting queried
def db_name_generate(root, path, label_volume = ""):
	if not label_volume:
		# Use pathlib to get the absolute path; this is vital for getting the volume label
		label_volume = get_volume_label(str(Path(path).resolve()))

	# Open the file to which we'd be saving dimensions
	return root + " - " + label_volume + os.extsep + "tsv", label_volume


def query_file(path_file, file_dimensions, label_volume, path_probe, mode_open, dict_files_failed, percentage_gather = False):
	root, extension = os.path.splitext(path_file)

	# Grab the part after the extension separator, and convert to lower case.
	# This is to ensure we don't skip files with extensions that Windows sets
	# to upper case. This is often the case with files downloaded from servers
	# or torrents.
	extension = (extension.rpartition(os.extsep)[2]).lower()

	# Only process video files
	if extension in (
		"av1", "avi", "divx", "mp4", "mkv", "m4v", "mpg", "mpeg", "mov", "rm", "vob", "wmv", "flv", "3gp", "rmvb", "webm",
		"dat", "mts"):
		# We're only gathering a headcount of files to query. Hence return once we increment the count.
		if percentage_gather:
			with mutex_count:
				query_file.total_count_percentage += 1

			return

		with mutex_count:
			query_file.total_count_files += 1

		db_open_standalone_file = None

		# Handle standalone files passed on the command line, instead of directories
		if (not file_dimensions) and (not label_volume):
			root_program, _ = os.path.splitext(sys.argv[0])

			file_dimensions_path, label_volume = db_name_generate(root_program, path_file)

			with mutex_file:
				try:
					# Open the db for the standalone file in question and keep so for the update below
					file_dimensions = io.open(file_dimensions_path, mode_open, encoding = "utf-8-sig")
				except OSError as error_io_open:
					# For reasons of efficiency, instead of calling lock_console_print_and_log(), we explicitly lock the
					# console access mutex to prevent back and forth locking for successive statements in the block below
					with mutex_console:
						print("Error", sys.exc_info())
						logging.error("Error", sys.exc_info())

						print(
							"Could not open \'" + file_dimensions_path + "\'. Aborting processing for \'" + path_file + "\'.")
						logging.error(
							"Could not open \'" + file_dimensions_path + "\'. Aborting processing for \'" + path_file + "\'.")

					return
				else:
					db_open_standalone_file = True

		# Update the db with the entry in question, rather than refreshing the whole file
		if mode_open == "a":
			if not query_file_update_check(path_file, file_dimensions):
				return

		with mutex_time:
			time_start = time.perf_counter_ns()

		# Probe metadata
		try:
			# from subprocess import check_output
			# TODO: For some files (.mts), more than one stream is picked up, despite asking
			#       only for the first video stream (v:0). Need to fix this in a later version.

			# ffprobe is capable of either probing only audio, or video in a single command. Until this is enhanced to be
			# otherwise, we'll have to make two runs - one each for video and audio. Until then, oblige.

			# Grab details for the video stream at index 0
			output_video = subprocess.run((path_probe, "-v", "error", "-select_streams", "v:0", "-show_entries",
			                               "format_tags=title:format=nb_streams,format_long_name:stream=codec_long_name,width,height",
			                               "-print_format", "default=noprint_wrappers=1:nokey=1", "-i", path_file),
			                              stdout = subprocess.PIPE, check = True, universal_newlines = True).stdout

			# Grab details for the audio stream at index 0
			output_audio = subprocess.run((path_probe, "-v", "error", "-select_streams", "a:0", "-show_entries",
			                               "stream=channels,codec_long_name", "-print_format",
			                               "default=noprint_wrappers=1:nokey=1", "-i", path_file),
			                              stdout = subprocess.PIPE, check = True, universal_newlines = True).stdout
		except subprocess.CalledProcessError as error_probe:
			# Update the dictionary with which file's probe failed and why
			dict_files_failed.update({path_file : str(sys.exc_info())})

			# For reasons of efficiency, instead of calling lock_console_print_and_log(), we explicitly lock the
			# console access mutex to prevent back and forth locking for successive statements in the block below
			with mutex_console:
				print(error_probe.output)
				print(error_probe.stderr)
				print("Error querying \'" + path_file + "\' for metadata")
				print("Error", sys.exc_info())

				logging.error(error_probe.output)
				logging.error(error_probe.stderr)
				logging.error("Error querying file \'" + path_file + "\': " + str(sys.exc_info()))

				print("Command that resulted in the exception: " + str(error_probe.cmd))
				logging.info("Command that resulted in the exception: " + str(error_probe.cmd))

			show_toast("Error", "Failed to probe \'" + path_file + "\'. Check the log.")
		# Handle any generic exception
		except:
			# Update the dictionary with which file's probe failed and why
			dict_files_failed.update({path_file : str(sys.exc_info())})

			# For reasons of efficiency, instead of calling lock_console_print_and_log(), we explicitly lock the
			# console access mutex to prevent back and forth locking for successive statements in the block below
			with mutex_console:
				print("Undefined exception")
				print("Error querying \'" + path_file + "\' for metadata")
				print("Error", sys.exc_info())

				logging.error("Undefined exception")
				logging.error("Error querying file \'" + path_file + "\': " + str(sys.exc_info()))

			show_toast("Error", "Failed to probe \'" + path_file + "\'. Check the log.")
		else:
			with mutex_time:
				query_file.total_time_queried += time.perf_counter_ns() - time_start
				time_start = time.perf_counter_ns()

			with mutex_file:
				# Strip off the trailing newline ffprobe spits in the output, before passing up
				save_video_information(file_dimensions, path_file, output_video.strip(), output_audio.strip(),
				                       label_volume)

			with mutex_time:
				query_file.total_time_db_save += time.perf_counter_ns() - time_start

			with mutex_count:
				# Keep count of the number of files processed
				query_file.total_count_queried += 1

			# print_and_log_spacer(query_file.count, path_file)
			lock_console_print_and_log(
				"Got metadata for file# " + "{:>4}".format(query_file.total_count_queried) + ((" of " + str(
					query_file.total_count_percentage)) if query_file.total_count_percentage else "") + ": \'" + path_file + "\'\n")

			# If the database was opened for a standalone file, close it
			if db_open_standalone_file:
				with mutex_file:
					file_dimensions.close()

		with mutex_count:
			if query_file.total_count_percentage:
				with mutex_console:
					percentage_completion_print(query_file.total_count_queried, query_file.total_count_percentage)


# Probe all audio streams
#		try:
#			# Probe the file for multiple audio streams. Done for accounting which
#			# file is hosting language audio not of interest, or even stereo audio,
#			# when surround is available.
#			print("Audio stream(s) for \'" + path_file + "\':")
#			logging.info("Audio stream(s) for \'" + path_file + "\':")
#
#			#output = subprocess.check_output([path_probe, "-v", "error", "-show_entries",
#			#                                  "stream=index,codec_name,codec_long_name,channels,channel_layout,sample_rate,codec_type:stream_tags=language,title",
#			#                                  "-select_streams", "a", "-of", "default=noprint_wrappers=1", path_file],
#			#                                 universal_newlines=True)
#			output = subprocess.check_output([path_probe, "-v", "error", "-show_entries",
#			                                  "stream=codec_type",
#			                                  "-select_streams", "a", "-of", "default=noprint_wrappers=1:nokey=1", path_file],
#			                                 universal_newlines=True)
#
#			print(output)
#			logging.info(output)
#
#
#		except subprocess.CalledProcessError as error_probe:
#			print(error_probe.output)
#			print(error_probe.stderr)
#			print("Error querying " + path_file + " for audio stream(s)")
#			print("Error", sys.exc_info())
#
#			logging.error(error_probe.output)
#        logging.error(error_probe.stderr)
#			logging.error("Error querying file " + path_file + str(sys.exc_info()))

query_file.total_count_files = 0
query_file.total_count_queried = 0
query_file.total_time_queried = 0
query_file.total_time_db_save = 0
query_file.total_count_percentage = 0


# Sorts the file containing in decreasing order of video dimension
def file_dimensions_sort(file_dimensions_path):
	error = True

	if platform.system() == "Windows":
		binary_sort = "sort.exe"
		option_reverse = "/R"
		option_output = "/O"
		# Since the path string is Unicode, it's required to convert "\"" to "\\".
		# Else, the sorting command below will fail accessing the path.
		file_dimensions_path = file_dimensions_path.replace('\\', '\\\\')
	else:
		binary_sort = "sort"
		option_reverse = ""
		option_output = "-o"

	output = None

	time_start = time.monotonic_ns()

	# Proceed only if we detected the sort command on a supported OS
	try:
		output = subprocess.run(
			(binary_sort, option_reverse, file_dimensions_path, option_output, file_dimensions_path),
			stdout = subprocess.PIPE, check = True, universal_newlines = True).stdout
	except subprocess.CalledProcessError as error_sort:
		print(error_sort.output)
		print(error_sort.stderr)
		logging.error(error_sort.output)
		logging.error(error_sort.stderr)

		print("Error sorting \'" + file_dimensions_path + "\'")
		print("Error", sys.exc_info())

		logging.error("Error sorting file \'" + file_dimensions_path + "\'" + str(sys.exc_info()))
	else:
		time_end = time.monotonic_ns()

		print(output)
		logging.info(output)

		error = False

		print(
			"Sorted \'" + file_dimensions_path + "\' in descending order of resolution stats in " + total_time_in_hms_get(
				time_end - time_start))
		logging.info(
			"Sorted \'" + file_dimensions_path + "\' in descending order of resolution stats in " + total_time_in_hms_get(
				time_end - time_start))

	return error


# For reading tags with UTF-8 encoding, we need a UTF-8 enabled console (or command prompt, in Windows parlance).
# This is applicable for writing tags as well. So warn the user to have the pre-requisite ready.
def sound_utf8_warning():
	print(
		"** Important: Non-ASCII characters in path and the video title require a UTF-8 enabled console/command prompt "
		"for reading and writing tags properly **\n\n")
	logging.info(
		"** Important: Non-ASCII characters in path and the video title require a UTF-8 enabled console/command prompt "
		"for reading and writing tags properly **\n\n")


# Parse command line arguments and return option and/or values of action
def cmd_line_parse(opt_update, opt_merge, opt_percentage):
	parser = argparse.ArgumentParser(
		description = "Reads metadata (resolution, size, title, etc.) from video files and dumps all in a tab "
		              "separated values (TSV) file, which can be opened with any program dealing in spreadsheets",
		add_help = True)
	parser.add_argument("-p", opt_percentage, required = False, action = "store_true",
	                    default = None, dest = "percentage",
	                    help = "Show the percentage of files completed (not the actual data processed; just the files")

	db_operate = parser.add_mutually_exclusive_group()
	db_operate.add_argument("-u", opt_update, required = False, action = "store_true", default = False,
	                        dest = "update_metadata_db",
	                        help = "Update the resolution statistics file with metadata for selected file(s)")
	db_operate.add_argument("-m", opt_merge, required = False, action = "store_true", default = False,
	                        dest = "merge_metadata",
	                        help = "Consolidates multiple (TSV) metadata files into a single file")

	result_parse, files_to_process = parser.parse_known_args()

	return result_parse.update_metadata_db, result_parse.merge_metadata, result_parse.percentage, files_to_process


# Spawn a pool of threads to query metadata
def threads_query(list_files, file_dimensions, label_volume, path_probe, mode_open, dict_files_failed, percentage_gather):
	with ThreadPool(COUNT_THREADS) as pool:
		pool.starmap(query_file, zip(list_files, itertools.repeat(file_dimensions), itertools.repeat(label_volume),
		                             itertools.repeat(path_probe), itertools.repeat(mode_open),
		                             itertools.repeat(dict_files_failed), itertools.repeat(percentage_gather)))


# Recursively process every directory passed on the command line
def process_dir(path, file_dimensions, label_volume, path_probe, mode_open, list_files_from_dir,
                dict_files_failed, percentage_gather):
	# A filter that tells not to walk through these directories
	filters = ("Extras", "Featurettes", "Soundtrack")

	# Only append the files to a list if it's empty. If we had been asked to report percentage,
	# the list would already be populated with paths of files to process.
	if not list_files_from_dir:
		# If it's a directory worth sniffing, walk through for files below
		for path_dir, _, file_names in os.walk(path):
			# Skip building resolution data for insignificant files, like found under "Extras",
			# or "Featurettes". These contain behind the scenes, deleted scenes or commentary.
			if os.path.basename(path_dir) not in filters:
				for file_name in file_names:
					list_files_from_dir.append(os.path.join(path_dir, file_name))
			else:
				print(
					"Directory filter \'" + os.path.basename(
						path_dir) + "\' flagged; skipping \'" + path_dir + "\'\n")
				logging.info(
					"Directory filter \'" + os.path.basename(
						path_dir) + "\' flagged; skipping \'" + path_dir + "\'\n")

	threads_query(list_files_from_dir, file_dimensions, label_volume, path_probe, mode_open,
	              dict_files_failed, percentage_gather)


# Process every path (irrespective of being a directory, or file) passed on the command line
def process_path(files_to_process, root, path_probe, mode_open, list_files_from_dir, percentage_gather = False):
	dict_files_failed = {}
	exit_code = 0
	file_standalone_path = None

	for path in files_to_process:
		file_dimensions_path, label_volume = db_name_generate(root, path)

		with io.open(file_dimensions_path, mode_open, encoding = "utf-8-sig") as file_dimensions:
			if os.path.isdir(path):
				process_dir(path, file_dimensions, label_volume, path_probe, mode_open, list_files_from_dir,
				            dict_files_failed, percentage_gather)
			else:
				# We got a standalone file, process it below
				file_standalone_path = path

		if not percentage_gather:
			if file_standalone_path and (mode_open == "a"):
				# The volume label and database name for standalone files could be anything as they are passed on the
				# command line as an independent path, rather than a directory to be recursed. Hence, for such files,
				# the onus will be on the querying routine to handle these parameters appropriately.
				query_file(file_standalone_path, None, None, path_probe, mode_open, dict_files_failed, percentage_gather)
			else:
				if file_standalone_path:
					# The mode option was not provided with a value to update. Crib.
					print(
						"\aOnly directories are queried for building a db from scratch. File \'" + file_standalone_path +
						"\'" + "will not be queried unless used only with the option to update the db.\n\n")
					logging.error(
						"\aOnly directories are queried for building a db from scratch. File \'" + file_standalone_path +
						"\'" + "will not be queried unless used only with the option to update the db.\n\n")

			# Once we're done writing dimensions for processed videos, sort the output file
			if file_dimensions_sort(file_dimensions_path):
				exit_code = 1

			# Print statistics on how long we took to query
			#
			# The accumulated time reported through time.perf_counter_ns() seems to be 10 times the actual time
			# taken! Scale accordingly before we pass it on to the user.
			if query_file.total_count_queried:
				print("\nQueried a total of " + str(query_file.total_count_queried) + "/" + str(
					query_file.total_count_files) + " files in " + total_time_in_hms_get(
					query_file.total_time_queried / 10) + " and took " + total_time_in_hms_get(
					query_file.total_time_db_save / 10) + " to commit details to the database")
				logging.info("\nQueried a total of " + str(query_file.total_count_queried) + "/" + str(
					query_file.total_count_files) + " files in " + total_time_in_hms_get(
					query_file.total_time_queried / 10) + " and took " + total_time_in_hms_get(
					query_file.total_time_db_save / 10) + " to commit details to the database")
			else:
				print("No files to query under '" +  path + "'")

	# Print a summary of failures
	if dict_files_failed:
		print("\a\n\nHere's a list of files that failed probing with the reason:\n")
		logging.info("\n\nHere's a list of files that failed probing with the reason:\n")

		for file, reason in dict_files_failed.items():
			print("File  : " + file)
			logging.info("File  : " + file)
			print("Reason: " + reason + "\n")
			logging.info("Reason: " + reason + "\n")

	return exit_code


# Consolidate metadata from files passed in a list into a target file
def files_merge(list_files, target):
	with io.open(target, "w", encoding = "utf-8-sig") as handle_write:
		for file in list_files:
			with io.open(file, "r", encoding = "utf-8-sig") as handle_read:
				shutil.copyfileobj(handle_read, handle_write)

				# Graceful closure
				handle_read.close()
		# Close the stream to flush buffers (and hence commit)
		handle_write.close()

	print("Merged \'" + str(list_files) + "\' into \'" + target + "\'")
	logging.info("Merged \'" + str(list_files) + "\' into \'" + target + "\'")


# Merge metadata dbs for video files from various disks/volumes
def db_metadata_merge(root, files_to_process):
	exit_code = 0

	merge = True

	for file in files_to_process:
		# Check if the files in question exist
		if not os.path.exists(file):
			# If there's even a single file that's bogus, bolt out
			print("\aInvalid/inaccessible file: \'" + file + "\'\n")
			logging.error("Invalid/inaccessible file: \'" + file + "\'\n")

			merge = False
			exit_code = 1

			break

	if merge:
		# Write the header to a file (which will be deleted after merging)
		db_name_header, _ = db_name_generate(root, None, "Header")

		# Write the header to a separate file (which will be deleted after merging metadata)
		with io.open(db_name_header, "w", encoding = "utf-8-sig") as handle_db_header:
			# The field widths *MUST* match the widths in save_video_information().
			# Else, it would break the merged file.
			# handle_db_header.write("{:>4}".format("Width") + "\t")
			# handle_db_header.write("{:>4}".format("Height") + "\t")
			# handle_db_header.write("{:>10}".format("Size") + "\t")
			# handle_db_header.write("{:>11}".format("Raw Size") + "\t")
			# handle_db_header.write("{:<50}".format("Video Codec Name") + "\t")
			# handle_db_header.write("{:>2}".format("Total # of Streams") + "\t")
			# handle_db_header.write("{:<35}".format("Container Name") + "\t")
			# handle_db_header.write("{:>1}".format("# of Audio Channels (@Index 0)") + "\t")
			# handle_db_header.write("{:<50}".format("Audio Codec Name (@Index 0)") + "\t")
			# handle_db_header.write("{:<255}".format("Title") + "\t")
			# handle_db_header.write("{:>1}".format("Ext. English Subtitle Availability") + "\t")
			# handle_db_header.write("{:<32}".format("Volume Label") + "\t")
			# handle_db_header.write("Path on Drive Label\n")

			handle_db_header.write("Width" + "\t")
			handle_db_header.write("Height" + "\t")
			handle_db_header.write("Size" + "\t")
			handle_db_header.write("Raw Size" + "\t")
			handle_db_header.write("Video Codec Name" + "\t")
			handle_db_header.write("Total # of Streams" + "\t")
			handle_db_header.write("Container Name" + "\t")
			handle_db_header.write("# of Audio Channels (@Index 0)" + "\t")
			handle_db_header.write("Audio Codec Name (@Index 0)" + "\t")
			handle_db_header.write("Title" + "\t")
			handle_db_header.write("Ext. English Subtitle Availability" + "\t")
			handle_db_header.write("Volume Label" + "\t")
			handle_db_header.write("Path on Drive Label\n")

			# Close the stream to flush buffers (and hence commit)
			handle_db_header.close()

		# Merge all the metadata files to temporary store
		db_name_merged_temp, _ = db_name_generate(root, None, "Merged - Temp")

		files_merge(files_to_process, db_name_merged_temp)

		# The name of the final file that will have a header followed by
		# sorted metadata
		db_name_merged, _ = db_name_generate(root, None, "Merged")

		if not file_dimensions_sort(db_name_merged_temp):
			merge_final = (db_name_header, db_name_merged_temp)

			# We have the sorted output. Create a new merge with the header file coming in
			# before merged and sorted metadata.
			files_merge(merge_final, db_name_merged)
		else:
			exit_code = 1

		# Wipe unnecessary files off storage
		if os.path.exists(db_name_merged_temp):
			os.remove(db_name_merged_temp)

			print("Deleted temporary file \'" + db_name_merged_temp + "\'")
			logging.info("Deleted temporary file \'" + db_name_merged_temp + "\'")
		if os.path.exists(db_name_header):
			os.remove(db_name_header)

			print("Deleted temporary file \'" + db_name_header + "\'")
			logging.info("Deleted temporary file \'" + db_name_header + "\'")

	return exit_code


# Change to the working directory of this Python script. Else, any dependencies will not be found.
def cwd_change(dir):
	os.chdir(os.path.dirname(os.path.abspath(dir)))

	print("Changing working directory to \'" + os.path.dirname(os.path.abspath(dir)) + "\'...\n")
	logging.info("Changing working directory to \'" + os.path.dirname(os.path.abspath(dir)) + "\'...\n")


def main(argv):
	exit_code = 0

	# We support only Windows and Unix like OSes
	if is_supported_platform():
		root, _ = os.path.splitext(sys.argv[0])

		logging_initialize(root)

		opt_update = "--update-metadata-db"
		opt_merge = "--merge-metadata"
		opt_percentage = "--percentage-completion"

		update_metadata, merge_metadata, percentage_show, files_to_process = cmd_line_parse(opt_update, opt_merge,
		                                                                                    opt_percentage)

		if files_to_process:
			# Remove duplicates from the source path(s)
			files_to_process = [*set(files_to_process)]

			if percentage_show and (update_metadata or merge_metadata):
				print(
					"Option \'" + opt_percentage + "\' cannot be applied along with \'" + opt_update + "\' or \'" + opt_merge + "\'")
				logging.info(
					"Option \'" + opt_percentage + "\' cannot be applied along with \'" + opt_update + "\' or \'" + opt_merge + "\'")
			else:
				cwd_change(sys.argv[0])

				if merge_metadata:
					exit_code = db_metadata_merge(root, files_to_process)
				else:
					sound_utf8_warning()

					# Default to opening the resolution statistics file in write mode (wipes out existing contents
					# of the file)
					mode_open = "w"

					if update_metadata:
						# We've been asked to update the resolutions stats with information for files passed on the
						# command line, instead of refreshing all stats
						mode_open = "a"

					# Based on the predefined path for an OS, look up where ffprobe is hiding ass
					path_probe = get_path_probe()

					# Check if the ffprobe binary exists
					if os.path.isfile(path_probe):
						percentage_gather = False

						# List for processing directories and standalone files passed on the command line
						list_files_from_dir = []
						list_files_standalone = []

						if percentage_show:
							# To report progress in percent, we need to gather the headcount of files to query
							percentage_gather = True

							print("Gathering file count for reporting percentage...\n\n")
							logging.info("Gathering file count for reporting percentage...\n\n")

							# No need to check for the return value; it's just a count we're getting
							process_path(files_to_process, root, path_probe, mode_open, list_files_from_dir,
							             percentage_gather)

							# We've already gathered the headcount, so flag accordingly
							percentage_gather = False

						print("\nInitiating probing...\n\n")
						logging.info("\nInitiating probing...\n\n")

						if process_path(files_to_process, root, path_probe, mode_open, list_files_from_dir,
						                percentage_gather):
							exit_code = 1
					else:
						print("\a\'" + path_probe + "\' not found")
						logging.error("\'" + path_probe + "\' not found")

						exit_code = 1
		else:
			print("\aThis program requires at least one argument")
			logging.error("This program requires at least one argument")

			exit_code = 1
	else:
		print("\aUnsupported OS")
		logging.error("Unsupported OS")

		exit_code = 1

	logging.shutdown()

	return exit_code


if __name__ == '__main__':
	main(sys.argv)
