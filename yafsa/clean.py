""" Tools for ETL/cleaning rank and stats data """

import numpy as np
import re


def normalize_player_names(namestr):
	"""
	Strips off all extra information from player names (e.g., opponent, date) and standardizes names (removes Jr., etc)
	:param namestr: string
	:return:
	"""
	return ' '.join(namestr.strip().split(' ')[:2]).strip(',.')


def strip_date_from_name(namestr):
	"""
	Removes date information (of the form mm/dd) from the end of a string name
	:param namestr: string
	:return:
	"""
	return re.split('\d+/\d+', namestr)[0]


def set_player_index(df, player_col, index_name):
	"""
	Sets player_col as the index, after normalization, with name index_name
	:param df: dataframe
	:param player_col: column containing player names
	:param index_name: name to use for index
	:return:
	"""
	df[player_col] = df[player_col].apply(normalize_player_names)

	df = (df.set_index(player_col)
			.rename_axis(index_name if index_name else player_col, axis=0))
	return df


def set_column_names(df, deduplicate=True):
	"""
	Removes trailing dates from all column names
	:param df: dataframe
	:param deduplicate: bool to indicate whether to deduplicate columns (keeping last) after removing trailing dates
	:return:
	"""
	df = df.rename(columns={col: strip_date_from_name(col) for col in df.columns})
	if deduplicate:
		df = df.loc[:, ~df.columns.duplicated(keep='last')]
	return df


def clean_data(df, player_col, index_name=None, select_cols=None, drop_cols=None, fill=None):
	"""
	Convenience function for cleaning data: subsetting, filling missing values, and setting index/columns
	:param df: DataFrame usually resulting from scraping stats or rankings
	:param player_col: column containing player names
	:param index_name: name to use for index (keeps player_col if None)
	:param select_cols: columns to be selected (defaults to all)
	:param drop_cols: columns to be dropped (defaults to none)
	:param fill: value in df to be filled with NaN
	:return:
	"""

	# subsetting
	if select_cols:
		select_cols = select_cols if isinstance(select_cols, list) else [select_cols]
		# ensure player_col is always selected
		df = df.filter(select_cols + [player_col], axis=1)
	if drop_cols:
		drop_cols = drop_cols if isinstance(drop_cols, list) else [drop_cols]
		# use select instead of drop here to avoid requiring every element of drop_cols to be in df
		df = df.select(lambda x: x not in drop_cols, axis=1)

	# fill missing values
	if fill is not None:
		df = df.replace(fill, np.nan)

	# set index and column names
	df = (df.pipe(set_player_index, player_col, index_name)
			.pipe(set_column_names, deduplicate=True))

	return df
