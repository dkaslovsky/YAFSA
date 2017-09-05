""" Class for scraping tables from HTML using BeautifulSoup """

import os
import ujson as json
from contextlib import closing
from bs4 import BeautifulSoup
from urllib2 import urlopen, URLError


class TableScraper(object):

	def __init__(self, header_rows_to_skip=None, chunk_size=None, replace_span_tag=True):
		"""
		:param header_rows_to_skip: number of leading rows of scraped table to skip
		:param chunk_size: number of rows to read at a time
		:param replace_span_tag: bool indicating whether to replace </span> with </th> for processing header labels
		"""
		self.header_rows_to_skip = header_rows_to_skip
		self.chunk_size = chunk_size
		self.replace_span_tag = replace_span_tag

	@staticmethod
	def _get_columns(row):
		return row.find_all('td')

	def _get_header_labels(self, header_row):
		"""
		Parse out column labels from header
		:param header_row:
		:return:
		"""
		if self.replace_span_tag:
			# there is likely a more efficient way to find/replace
			header_soup = BeautifulSoup(str(header_row).replace('</span>', '</th>'), 'html.parser')
		else:
			header_soup = header_row
		header_labels = [h.text.strip() for h in header_soup.find_all('th')]
		return header_labels

	def _parse_rows(self, rows, labels):
		"""
		Build list of dict records
		:param rows:
		:param labels: header labels to be used as keys
		:return:
		"""
		return [{label: td.text.strip() for label, td in zip(labels, self._get_columns(row))} for row in rows]

	def _parse_table(self, urlhandle):
		page = urlhandle.read()
		soup = BeautifulSoup(page, 'html.parser')
		table = soup.find('table')
		rows = table.find_all('tr')
		if self.header_rows_to_skip:
			rows = rows[self.header_rows_to_skip:]
		header = rows.pop(0)
		header_labels = self._get_header_labels(header)
		records = self._parse_rows(rows, header_labels)
		return records

	def _parse_table_chunked(self, urlhandle):
		raise NotImplementedError('Chunked reading is not implemented yet, initialize %s with chunk_size=None'
		                          % self.__class__.__name__)

	def scrape_table(self, url):
		if self.chunk_size and self.chunk_size > 0:
			table_parser = self._parse_table_chunked
		else:
			table_parser = self._parse_table

		try:
			with closing(urlopen(url)) as urlhandle:  # urllib2 doesn't implement 'with' so need to use contextlib
				records = table_parser(urlhandle)
		except (URLError, ValueError):
			print 'Could not open url: %s' % url
			records = {}
		return records

	def write_to_file(self, records, outdir, outfile):
		if not os.path.exists(outdir):
			os.makedirs(outdir)
		outfile = '%s.%s' % (os.path.splitext(outfile)[0], 'json')  # ensure file has extension
		full_file_name = os.path.join(outdir, outfile)
		with open(full_file_name, 'w') as f:
			f.write(json.dumps(records))
		return full_file_name
