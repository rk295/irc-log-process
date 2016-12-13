#!/usr/bin/env python

import sys
import os
import logging
import re
import json
import requests
from datetime import datetime
from pprint import pprint

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                    datefmt="%m-%d %H:%M")

logger = logging.getLogger(os.path.basename(__file__))


def post_data(data, url):

    r = requests.post(url, data=data, headers={
                      'Content-Type': 'application/octet-stream'})

    if r.status_code != 200:
        logger.debug("HTTP Status from elastic was %s" % (r.status_code))
        logger.debug("Response body from elastic was %s" % (r.json()))
    else:
        logger.debug("Message posted to ElasticSearch successfully")


logger.debug("Starting...")

log_dir = os.getenv("LOG_DIR", "/u01/home/robin/GIT/irc-import/logs")

# Tested here: https://regex101.com/r/FNxiHo/1
message_re = "^\[(?P<hours>\d\d):(?P<mins>\d\d):(?P<seconds>\d\d)\] (?:\*\*\* )?(?:\* )?(?:Mode )?(?P<name>[a-zA-Z0-9|]+)(?:_)?(?:>)? (?P<message>.*)$"
message_re_comp = re.compile(message_re)

elastic_index = "slack"
elastic_type = "message"

elastic_url = "http://localhost:9200/_bulk"
logger.debug("Will use elastic search URL %s" % elastic_url)


for file in os.listdir(log_dir):

    # Reset this with each new file.
    bulk_string = ""

    logger.debug("Starting to process file=%s" % file)

    # Rip a bunch of rubbish from the filename, leaving with YYYY-MM-DD
    # Although no zero padded!
    day_date = file.replace("soton.", "").replace(
        ".txt", "").replace("fermat.", "")

    # Split the filename date on the - delimitter
    (year, month, day) = day_date.split("-")

    # Reconstruct but zero pad this time
    date_string = "{0}-{1:02d}-{2:02d}".format(year, int(month), int(day))
    logger.debug("date_string=%s" % date_string)

    # Figure out the file path
    file_path = log_dir + "/" + file

    # Each new line in the bulk load needs a index line
    # this holds that.
    index_obj = {}

    # Construct the index object, and push into the parent index_obj
    index = {}
    index["_index"] = "slack-" + year
    index["_type"] = elastic_type
    index_obj["index"] = index

    bulk_string_header = json.dumps(index_obj) + "\n"

    with open(file_path) as f:
        for line in f:

            msg = {}
            msg["file_name"] = file

            # Ignore any UTF8 errors
            line = line.decode("utf8", errors="ignore")

            # Remove the newline
            line = line.rstrip()
            msg["orig_line"] = line
            logger.debug("Read line=%s" % line)

            # Run the big regex from above and skip if parsing failed
            result = message_re_comp.match(line)
            if result is None:
                logger.error("Failed to process this line")
                continue

            # Pull out the bits we want, via named capture groups
            hours = result.group('hours')
            mins = result.group('mins')
            seconds = result.group('seconds')
            username = result.group('name')
            message = result.group('message')

            logger.info("user=%s said=%s" % (username, message))

            # Construct an ISO format timestring (in iso_ts)
            ts_string = "{0} {1}:{2}:{3}".format(
                date_string, hours, mins, seconds)
            iso_ts = datetime.strptime(
                ts_string, '%Y-%m-%d %H:%M:%S').isoformat()

            logger.debug("iso_ts=%s" % iso_ts)

            # Push some useful stuff into the message object
            msg["@timestamp"] = iso_ts
            msg["user_name"] = username
            msg["text"] = message

            # Add the bulk_string_header defined above, with this msg and importantly a newline.
            bulk_string = bulk_string + bulk_string_header + json.dumps(msg) + "\n"

    # Now we've read all the lines from the file, add a final newline
    bulk_string = bulk_string + "\n"
    # And push to elastic
    post_data(bulk_string, elastic_url)

