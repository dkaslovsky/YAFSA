""" Script to scrape weekly fantasy football stats from thehuddle.com """

import os
import time
from yafsa.scrape import TableScraper, write_to_file


# URL specification
URL_BASE = 'http://thehuddle.com/stats'
YEAR = '2016'
FREQ = 'plays_weekly.php?'
URL = '/'.join([URL_BASE, YEAR, FREQ])

# args to loop over
WEEKS = range(1, 18)
POSITIONS = ['QB', 'RB', 'WR', 'TE', 'PK']
CCS = '5'  # corresponds to Yahoo scoring

# arguments for TableScraper
HEADER_ROWS_TO_SKIP = 1  # skip this many leading rows of each table (URL specific)
CHUNK_SIZE = None  # read each table in this many chunks

# directory for writing data
OUTDIR = os.path.join(os.path.dirname(__file__), 'data', 'stats')


if __name__ == '__main__':

	ts = TableScraper(header_rows_to_skip=HEADER_ROWS_TO_SKIP, chunk_size=CHUNK_SIZE)

	file_count = 0
	ccs = '%s%s' % ('ccs=', CCS)
	for week in WEEKS:
		wk = '%s%s' % ('week=', week)
		for position in POSITIONS:
			pos = '%s%s' % ('pos=', position)
			args = '&'.join([ccs, pos, wk])
			url = '%s%s' % (URL, args)

			data = ts.scrape_table(url)

			# optional sleep to avoid hitting URL_BASE too quickly
			time.sleep(1)

			# write file
			file_name = '_'.join([YEAR, wk, pos])
			full_file_name = write_to_file(data, OUTDIR, file_name)
			file_count += 1
			print 'Wrote file %i: %s' % (file_count, full_file_name)
