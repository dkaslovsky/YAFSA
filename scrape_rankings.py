""" Script to scrape weekly fantasy football rankings from fantasypros.com """

import os
import time
import pandas as pd
from yafsa.scrape import TableScraper, write_table_to_csv


# TODO: eliminate use of pandas once ts.write_to_file is implemented, resave data in record format


# URL specification
URL_BASE = 'http://partners.fantasypros.com/external/widget/nfl-staff-rankings.php?'
SCORING = 'STD'
YEAR = 2016
URL = '&'.join([
	URL_BASE,
	'%s=%s' % ('year', YEAR),
	'%s=%s' % ('scoring', SCORING),
])

# args to loop over
POSITIONS = ['QB', 'RB', 'WR', 'TE', 'K']
WEEKS = range(1, 18)
SOURCES = range(1, 5)

# arguments for TableScraper
HEADER_ROWS_TO_SKIP = 0  # skip this many leading rows of each table (URL specific)
CHUNK_SIZE = None  # read each table in this many chunks

# Directory to write results
OUTDIR = os.path.join(os.path.abspath(os.path.curdir), 'data', 'rankings')  # directory for writing data


if __name__ == '__main__':

	ts = TableScraper(header_rows_to_skip=HEADER_ROWS_TO_SKIP, chunk_size=CHUNK_SIZE)

	file_count = 0
	for week in WEEKS:
		wk = '%s=%s' % ('week', week)
		for position in POSITIONS:
			pos = '%s=%s' % ('position', position)
			for source in SOURCES:
				src = '%s=%s' % ('source', source)
				args = '&'.join(['', pos, wk, src])
				url = '%s%s' % (URL, args)

				data = ts.scrape_table(url)
				if not data:  # source out of range
					break

				# optional sleep to avoid hitting URL_BASE too quickly
				time.sleep(1)

				# write file
				df = pd.DataFrame(data)
				file_name = '%s_%s' % (YEAR, args)
				full_file_name = write_table_to_csv(df, OUTDIR, file_name)
				file_count += 1
				print 'Wrote file %i: %s' % (file_count, full_file_name)
