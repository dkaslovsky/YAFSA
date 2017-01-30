import os
import time

from yafsa import read_table


# URL specification
URL_BASE = 'http://thehuddle.com/stats'
YEAR = '2016'
FREQ = 'plays_weekly.php?'
URL = '/'.join([URL_BASE, YEAR, FREQ])

# args to loop over
WEEKS = range(1, 18)
POSITIONS = ['QB', 'RB', 'WR', 'TE', 'PK']
CCS = '5'  # corresponds to Yahoo scoring

# arguments for read_table
HEADER_ROWS_TO_SKIP = 1  # skip this many leading rows of each table (URL specific)
CHUNK_SIZE = None  # read each table in this many chunks

OUTDIR = os.path.join(os.path.abspath(os.path.curdir), 'data', 'stats')  # directory for writing data


def write_table_to_csv(df, outdir, outfile):
	if not os.path.exists(outdir):
		os.makedirs(outdir)
	outfile = '%s.%s' % (os.path.splitext(outfile)[0], 'csv')  	# ensure file has extension
	full_file_name = os.path.join(outdir, outfile)
	df.to_csv(full_file_name)
	return full_file_name


if __name__ == '__main__':

	file_count = 0
	ccs = '%s%s' % ('ccs=', CCS)
	for week in WEEKS:
		wk = '%s%s' % ('week=', week)
		for position in POSITIONS:
			pos = '%s%s' % ('pos=', position)
			args = '&'.join([ccs, pos, wk])
			url = '%s%s' % (URL, args)
			df = read_table(url, chunk_size=CHUNK_SIZE, header_rows_to_skip=HEADER_ROWS_TO_SKIP)
			time.sleep(1)

			# write file
			file_name = '%s_%s' % (YEAR, args)
			full_file_name = write_table_to_csv(df, OUTDIR, file_name)
			file_count += 1
			print 'Wrote file %i: %s' % (file_count, full_file_name)
