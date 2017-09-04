""" """

import os
import numpy as np
import pandas as pd

from metrics import dcg


BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
STATS_PATH = 'data/stats'
RANK_PATH = 'data/rankings'
FILE_PARAMS = {
	'year': '2016',
	'week': '1',
	'pos': 'WR'
}

# for now we'll be dumb and hard code the file names, including source for the ranks
# TODO: look into standardizing position/pos
STATS_FILE = '%s_week=%s_pos=%s.json' % (FILE_PARAMS['year'], FILE_PARAMS['week'], FILE_PARAMS['pos'])
RANK_FILE = '%s_week=%s_position=%s_source=1.json' % (FILE_PARAMS['year'], FILE_PARAMS['week'], FILE_PARAMS['pos'])




# TODO: check for divide by zero
# TODO: should this class use dicts instead of dfs?
class Ranker(object):

	def __init__(self, metric, normalize=True):
		self.metric = metric
		self.normalize = normalize
		# populated by fit
		self.points_ = None
		self.max_score_ = None

	def fit(self, points):
		if isinstance(points, pd.Series):
			self.points_ = points.rename('points')
		elif isinstance(points, pd.DataFrame):
			self.points_ = points.rename(columns={col: 'points' for col in points.columns})
		else:
			raise ValueError('points must be a Pandas DataFrame or Series')

		if self.normalize:
			self.max_score_ = self.metric(sorted(self.points_.values.ravel().tolist(), reverse=True))
		return self

	def score(self, ranks):
		if not isinstance(ranks, pd.Series):
			raise ValueError('ranks must be a Pandas Series')
		df = (pd.concat([self.points_, ranks.rename('ranks')], axis=1)  # join on the index
				.dropna(axis=0, how='any')
				.astype(int)
				.sort_values('ranks'))
		scored = self.metric(df['points'].tolist())
		if self.normalize:
			scored /= self.max_score_
		return scored


def normalize_player_names(namestr):
	"""
	Strips off all extra information from player names (e.g., opponent, date) and standardizes names (removes Jr., etc)
	:param namestr:
	:return:
	"""
	return ' '.join(namestr.strip().split(' ')[:2]).strip(',.')


def clean_data(df, player_col, index_name='Player', select_cols=None, drop_cols=None, fill=None):

	if select_cols:
		select_cols = select_cols if isinstance(select_cols, list) else [select_cols]
		df = df.select(lambda x: x in select_cols + [player_col], axis=1)
	if drop_cols:
		df = df.drop(drop_cols, axis=1)
	if fill is not None:
		df = df.replace(fill, np.nan)

	df[player_col] = df[player_col].apply(normalize_player_names)

	df = (df.rename(columns={player_col: index_name})
			.set_index(index_name))

	return df




if __name__ == '__main__':

	# ETL
	stats = (pd.read_json(os.path.join(BASE_DIR, STATS_PATH, STATS_FILE))
			   .pipe(clean_data, player_col='PLAYER', select_cols='FPTS'))

	ranks = (pd.read_json(os.path.join(BASE_DIR, RANK_PATH, RANK_FILE))
			   .pipe(clean_data, player_col='Player (matchup)', drop_cols='Rank', fill=''))


	
	# TODO: how to choose k?  move into class so k is part of state?
	METRIC = lambda x: dcg(x, k=30, numerator='rel', normalized=False)


	ranker = Ranker(METRIC, normalize=True)
	ranker = ranker.fit(stats)

	print ranks.apply(ranker.score)
