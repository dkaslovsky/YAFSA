""" Class for scraping tables from HTML using BeautifulSoup """

import os
import ujson as json
from contextlib import closing
from bs4 import BeautifulSoup
from urllib2 import urlopen, URLError


# TODO: move away from urllib2 to requests


class TableScraper(object):

	def __init__(self, header_rows_to_skip=0, chunk_size=None, replace_span_tag=True):
		"""
		:param header_rows_to_skip: number of leading rows of scraped table to skip
		:param chunk_size: number of rows to read at a time
		:param replace_span_tag: bool indicating whether to replace </span> with </th> for processing header labels
		"""
		self.header_rows_to_skip = int(header_rows_to_skip)
		self.chunk_size = max(0, chunk_size)
		self.replace_span_tag = replace_span_tag

	def scrape_table(self, url):
		"""
		Scrape table from url into list of records, one for each row
		:param url: string
		:return:
		"""
		if self.chunk_size > 0:
			table_parser = self._parse_table_chunked
		else:
			table_parser = self._parse_table

		try:
			# urllib2 doesn't implement 'with' so need to use contextlib
			with closing(urlopen(url)) as urlhandle:
				records = table_parser(urlhandle)
		except (URLError, ValueError):
			print 'Could not open url: %s' % url
			records = []
		except IndexError as e:
			print e.message
			records = []
		return records

	def write_to_file(self, records, outdir, outfile):
		if not os.path.exists(outdir):
			os.makedirs(outdir)
		outfile = '%s.%s' % (os.path.splitext(outfile)[0], 'json')  # ensure file has extension
		full_file_name = os.path.join(outdir, outfile)
		with open(full_file_name, 'w') as f:
			f.write(json.dumps(records))
		return full_file_name

	@staticmethod
	def _parse_columns(row):
		"""
		Parse columns from a single row
		:param row:
		:return:
		"""
		return row.find_all('td')

	def _get_column_labels(self, header_row):
		"""
		Parse column labels from header
		:param header_row:
		:return:
		"""
		if self.replace_span_tag:
			# there is likely a more efficient way to find/replace
			header_soup = BeautifulSoup(str(header_row).replace('</span>', '</th>'), 'html.parser')
		else:
			header_soup = header_row
		column_labels = [h.text.strip() for h in header_soup.find_all('th')]
		return column_labels

	def _prepare_rows(self, rows):
		"""
		Remove leading rows and ensure rows is not empty
		:param rows: list of rows (will be mutated)
		:return:
		"""
		rows = rows[self.header_rows_to_skip:]
		if not rows:
			raise IndexError('No rows to process: try increasing chunk_size or reducing header_rows_to_skip')
		return rows

	def _parse_rows(self, rows, labels):
		"""
		Build list of dict records
		:param rows:
		:param labels: column labels to be used as keys
		:return:
		"""
		return [{label: td.text.strip() for label, td in zip(labels, self._parse_columns(row))} for row in rows]

	def _parse_table(self, urlhandle):
		"""
		Parse table into row records
		:param urlhandle:
		:return:
		"""
		page = urlhandle.read()
		soup = BeautifulSoup(page, 'html.parser')
		table = soup.find('table')
		rows = table.find_all('tr')
		rows = self._prepare_rows(rows)
		header = rows.pop(0)
		column_labels = self._get_column_labels(header)
		records = self._parse_rows(rows, column_labels)
		return records

	def _parse_table_chunked(self, urlhandle):
		"""
		Parse table into row records, reading from the url in chunks
		:param urlhandle:
		:return:
		"""
		all_records = []

		# initially column labels have not been parsed
		n_columns = 0
		column_labels = None
		# initially there is no split row to reconcile between chunks
		reconcile_split_row = False
		split_row_part1 = None
		joined_record = None

		while True:
			# read chunk of data, break out of loop if no chunks remain
			chunk = urlhandle.read(self.chunk_size)
			if not chunk:
				break

			# read current chunk and extract rows
			soup = BeautifulSoup(chunk, 'html.parser')
			rows = soup.find_all('tr')

			# extract header information if it has not yet been parsed
			# (should be when the current chunk is the first chunk)
			if column_labels is None:
				rows = self._prepare_rows(rows)
				header = rows.pop(0)
				column_labels = self._get_column_labels(header)
				n_columns = len(column_labels)

			# if previous chunk's last row extends into current chunk,
			# join the two parts of the split row and parse the joined row
			if reconcile_split_row and split_row_part1:
				(split_row_part2, _, _) = chunk.partition('</tr>')
				joined_soup = BeautifulSoup('<tr>%s%s</tr>' % (split_row_part1, split_row_part2),
				                            'html.parser')
				joined_row = joined_soup.find_all('tr')
				joined_record = self._parse_rows(joined_row, column_labels)

			# parse rows and combine with reconciled row if necessary
			records = self._parse_rows(rows, column_labels)
			if reconcile_split_row and joined_record:
				records = joined_record + records

			# check if last record is incomplete (split between two chunks)
			reconcile_split_row = (len(records[-1]) < n_columns)
			# if yes, remove last (incomplete) record and save off the incomplete
			# row data to be reconciled in the next iteration
			if reconcile_split_row:
				records.pop()
				(_, _, split_row_part1) = chunk.rpartition('<tr>')

			all_records.extend(records)

		return all_records
