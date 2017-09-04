""" Driver script for analyzing rankings """

import os
import pandas as pd

from functools import partial
from yafsa.clean import clean_data
from yafsa.rank import Ranker, dcg


BASE_DIR = os.path.dirname(__file__)
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


if __name__ == '__main__':

	# ETL
	stats = (pd.read_json(os.path.join(BASE_DIR, STATS_PATH, STATS_FILE))
			   .pipe(clean_data, player_col='PLAYER', select_cols='FPTS'))

	ranks = (pd.read_json(os.path.join(BASE_DIR, RANK_PATH, RANK_FILE))
			   .pipe(clean_data, player_col='Player (matchup)', drop_cols='Rank', fill=''))

	_dcg = partial(dcg, k=30, numerator='rel', normalized=False)

	ranker = Ranker(_dcg, normalize=True)
	ranker = ranker.fit(stats)

	scores = ranker.score(ranks)
	print scores
