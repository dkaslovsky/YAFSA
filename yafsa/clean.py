""" Tools for ETL/cleaning rank and stats data """

import numpy as np
import re


def normalize_player_names(namestr):
	"""
	Strips off all extra information from player names (e.g., opponent, date) and standardizes names (removes Jr., etc)
	:param namestr:
	:return:
	"""
	return ' '.join(namestr.strip().split(' ')[:2]).strip(',.')


def clean_data(df, player_col, index_name='Player', select_cols=None, drop_cols=None, fill=None):
	"""
	Convenience function for subsetting a dataframe, filling missing values, and indexing by normalized player names
	:param df: DataFrame usually resulting from scraping stats or rankings
	:param player_col: column containing player names
	:param index_name: name to use for index
	:param select_cols: columns to be selected (defaults to all)
	:param drop_cols: columns to be dropped (defaults to none)
	:param fill: value in df to be filled with NaN
	:return:
	"""

	if select_cols:
		select_cols = select_cols if isinstance(select_cols, list) else [select_cols]
		# ensure player_col is always selected
		df = df.select(lambda x: x in select_cols + [player_col], axis=1)
	if drop_cols:
		drop_cols = drop_cols if isinstance(drop_cols, list) else [drop_cols]
		# use select instead of drop to avoid requiring every element to be in df
		df = df.select(lambda x: x not in drop_cols, axis=1)
	if fill is not None:
		df = df.replace(fill, np.nan)

	df[player_col] = df[player_col].apply(normalize_player_names)

	# relabel player_column with index_name, set as index, and remove dates from remaining column names
	df = (df.rename(columns={player_col: index_name})
			.set_index(index_name)
	        .rename(columns={col: re.split('\d+/\d+', col)[0] for col in df.columns}))

	return df