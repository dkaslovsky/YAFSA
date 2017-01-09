import pandas as pd

from urllib import urlopen
from bs4 import BeautifulSoup


HEADER_ROWS_TO_SKIP = 1

def get_columns(row):
	return row.find_all('td')


def parse_rows(rows, labels):
	return [{label: td.text.strip() for label, td in zip(labels, get_columns(row))} for row in rows]


def read_table(url, chunk_size=None):

	# TODO: catch exceptions for bad url
	urlhandle = urlopen(url)

	if chunk_size:
		return chunked_read_table(urlhandle, chunk_size)

	page = urlhandle.read()

	soup = BeautifulSoup(page)
	table = soup.find('table')
	rows = table.find_all('tr')
	rows = rows[HEADER_ROWS_TO_SKIP:]

	header = rows.pop(0)
	column_labels = [h.text.strip() for h in  header.find_all('th')]
	records = parse_rows(rows, column_labels)
	return pd.DataFrame(records).set_index('PLAYER')


def chunked_read_table(urlhandle, chunk_size):

	# TODO: errorcheck: chunk_size > 0, etc

	dfs = []  # store dataframe of records for each chunk
	reconcile_split_row = False  # initially there are now split rows to reconcile between chunks
	chunk_num = 0

	while True:
		chunk = urlhandle.read(chunk_size)
		chunk_num += 1
		if not chunk:
			break

		# read current chunk and extract all rows
		soup = BeautifulSoup(chunk)
		rows = soup.find_all('tr')
		if not rows:
			continue

		# if the current chunk is the first chunk, extract header information
		if chunk_num == 1:
			rows = rows[HEADER_ROWS_TO_SKIP:]
			header = rows.pop(0)
			column_labels = [h.text.strip() for h in header.find_all('th')]
			n_columns = len(column_labels)

		# if previous chunk's last row extends into current chunk, join the two an parse the joined row
		if reconcile_split_row and split_row_part1:
			(split_row_part2, _, _) = chunk.partition('</tr>')
			joined_row = BeautifulSoup('<tr>%s%s</tr>' % (split_row_part1, split_row_part2)).find_all('tr')
			joined_record = parse_rows(joined_row, column_labels)

		# parse rows
		records = parse_rows(rows, column_labels)
		# combine with reconciled row if necessary
		if reconcile_split_row and joined_record:
			records = joined_record + records

		# check if last record is split between two chunks (number of extacted columns will be < n_columns)
		reconcile_split_row = (len(get_columns(rows[-1])) < n_columns)

		# remove last (incomplete) record and save the incomplete row data
		if reconcile_split_row:
			records.pop()
			(_, _, split_row_part1) = chunk.rpartition('<tr>')

		dfs.append(pd.DataFrame(records).set_index('PLAYER'))

	return pd.concat(dfs)
