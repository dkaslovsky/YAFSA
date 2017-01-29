import pandas as pd

from urllib2 import urlopen, URLError
from bs4 import BeautifulSoup


def get_columns(row):
	return row.find_all('td')


def parse_rows(rows, labels):
	return [{label: td.text.strip() for label, td in zip(labels, get_columns(row))} for row in rows]


def read_table(url, chunk_size=None, header_rows_to_skip=None):

	try:
		urlhandle = urlopen(url)
	except (URLError, ValueError):
		print 'Could not open url: %s' % url
		return pd.DataFrame()

	if chunk_size:
		return chunked_read_table(urlhandle, chunk_size, header_rows_to_skip)
	else:
		page = urlhandle.read()

		soup = BeautifulSoup(page, 'html.parser')
		table = soup.find('table')
		rows = table.find_all('tr')
		if header_rows_to_skip:
			rows = rows[header_rows_to_skip:]

		header = rows.pop(0)
		column_labels = [h.text.strip() for h in header.find_all('th')]
		records = parse_rows(rows, column_labels)
		return pd.DataFrame(records).set_index('PLAYER')


def chunked_read_table(urlhandle, chunk_size, header_rows_to_skip):
	raise NotImplementedError('Chunked reading of a table is not yet implemented, use read_table(url, chunk_size=None)')
