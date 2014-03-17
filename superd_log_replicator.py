#!/usr/bin/env python
#
# Supervisord Log Replicator:
# This script assumes it will be run in supervisord and 
# restarted after every execution attempt.
#
# author: Ed Silber

import datetime
import linecache
import random
import time

# File locations
source_file = '/var/log/messages'
destination_file = '/opt/iepug_supervisord/files/processed_log'
line_cache_file = '/opt/iepug_supervisord/files/line_cache'

def process_file():
	# Mark starting point and initialize line count
	start_datetime = datetime.datetime.now()
	line_count = 0

	# Check if the line cache exists and create it if it does not
	try:
		open(line_cache_file, 'r').close()
	except IOError:
		open(line_cache_file, 'a').close()

	# Artificial line limit for reading file_to_scan
	line_limit = random.randint(1,10)

	# Open the source file in read mode
	source_log = open(source_file, 'r')

	# Open cache file, read a line and close it
	cache = open(line_cache_file, 'r')
	last_line_processed = cache.readline()
	cache.close()

	# Open destination log in append mode
	destination_log = open(destination_file, 'a')

	# If the there is no last line processed, set it to 1;
	# otherwise, convert what was read to int
	if last_line_processed == '':
		last_line_processed = 1
	else:
		last_line_processed = int(last_line_processed)

	# Read one line for each line
	for i in range(line_limit):
		# Calculate the line to continue on
		line_no = last_line_processed + line_limit
		# Read in one line...
		log_line = linecache.getline(source_file, line_no)
		# ...and write it to the destination
		destination_log.write(log_line)

		# Overwrite the cache file with our newest position
		cache = open(line_cache_file, 'w')
		cache.write(str(line_no))
		cache.flush()
		cache.close()

		# Keep track of how many lines have been read
		line_count += 1

	# Close the destination file
	destination_log.flush()
	destination_log.close()

	# Save ending point
	end_datetime = datetime.datetime.now()

	# Save meta data only if we processed something
	if line_count > 0:
		# Save meta data, including the last doc we processed successfully
		log_meta_data(start_datetime, end_datetime, line_count, line_no)

def log_meta_data(start_dt, end_dt, count, last_line):
	# Setup a doc to track supervisord metrics
	print 'Log meta data:'
	print '\tStart datetime: %s' % (str(start_dt))
	print '\tLast line number %i' % (last_line)
	print '\tProcessed %i lines' % (count)
	print '\tEnd datetime: %s' % (str(end_dt))

if __name__ == "__main__":
	process_file()
	# Sleep for a bit when completed
	# 	If this is commented out, supervisord will immediately restart
	# 	this script again when it finishes
	time.sleep(1)
