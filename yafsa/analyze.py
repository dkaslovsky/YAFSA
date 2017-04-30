"""

"""

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

# TODO: how to choose k?  move into class so k is part of state?
METRIC = lambda x: dcg(x, k=30, numerator='rel', normalized=False)


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
	return ' '.join(namestr.strip().split(' ')[:2]).strip(',.')


if __name__ == '__main__':

	# TODO: handle dropping columns in more generic manner

	stats = (pd.read_json(os.path.join(BASE_DIR, STATS_PATH, STATS_FILE))
	            .pipe(lambda x: x[['FPTS', 'PLAYER']])
				.set_index('PLAYER'))
	stats.index = stats.index.map(normalize_player_names)

	ranks = (pd.read_json(os.path.join(BASE_DIR, RANK_PATH, RANK_FILE))
			   .drop(['Rank', 'StaffComposite9/11'], axis=1)
			   .set_index('Player (matchup)')
	           .replace('', np.nan))
	ranks.index = ranks.index.map(normalize_player_names)

	ranker = Ranker(METRIC, normalize=True)
	ranker = ranker.fit(stats)

	print ranks.apply(ranker.score)
