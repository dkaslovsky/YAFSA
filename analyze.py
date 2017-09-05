""" Driver script for analyzing rankings """

import os
import pandas as pd

from functools import partial
from yafsa.clean import clean_data
from yafsa.rank import Ranker, dcg


#### NOTE: WORK IN PROGRESS ###


BASE_DIR = os.path.dirname(__file__)
STATS_PATH = os.path.join('data', 'stats')
RANK_PATH = os.path.join('data', 'rankings')

# args to loop over
YEAR = 2016
POSITIONS = ['QB', 'RB', 'WR', 'TE']
WEEKS = range(1, 18)
SOURCES = range(1, 5)

# define ranking metric and ranker
_dcg = partial(dcg, k=30, numerator='rel')
ranker = Ranker(_dcg, normalize=True)


if __name__ == '__main__':

	position_dfs = {}

	for position in POSITIONS:

		dfs = []

		for week in WEEKS:

			# ETL stats
			stats_file = '%s_week=%s_pos=%s.json' % (YEAR, week, position)
			stats = (pd.read_json(os.path.join(BASE_DIR, STATS_PATH, stats_file))
			           .pipe(clean_data, player_col='PLAYER', select_cols='FPTS'))

			# ETL ranks
			ranks_list = []
			for source in SOURCES:
				rank_file = '%s_week=%s_position=%s_source=%s.json' % (YEAR, week, position, source)
				ranks = (pd.read_json(os.path.join(BASE_DIR, RANK_PATH, rank_file))
				           .pipe(clean_data, player_col='Player (matchup)',
				                 drop_cols=['Rank', 'FantasyProsAll Experts'], fill=''))
				ranks_list.append(ranks)
			ranks = pd.concat(ranks_list, axis=1)

			ranker = ranker.fit(stats)
			scores = ranker.score(ranks)
			dfs.append(scores)

		# TODO: this doesn't work because we need to dedupe columns
		position_dfs[position] = pd.concat(dfs, axis=1)
