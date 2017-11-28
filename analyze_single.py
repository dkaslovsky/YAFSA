""" Driver script for analyzing rankings """

import os
import pandas as pd

from yafsa.clean import clean_data
from yafsa.score import DCGScorer


BASE_DIR = os.path.dirname(__file__)
STATS_PATH = os.path.join('data', 'stats')
RANK_PATH = os.path.join('data', 'rankings')

# for now we'll just hard code the file parameters for development
FILE_PARAMS = {
	'year': '2016',
	'week': '1',
	'pos': 'WR',
	'source': '1'
}

STATS_FILE = '%s_week=%s_pos=%s.json' \
             % (FILE_PARAMS['year'], FILE_PARAMS['week'], FILE_PARAMS['pos'])
RANK_FILE = '%s_week=%s_position=%s_source=%s.json' \
            % (FILE_PARAMS['year'], FILE_PARAMS['week'], FILE_PARAMS['pos'], FILE_PARAMS['source'])

scorer = DCGScorer(k=25, numerator='exp', normalize=True)


if __name__ == '__main__':

	# ETL
	stats = (pd.read_json(os.path.join(BASE_DIR, STATS_PATH, STATS_FILE))
			   .pipe(clean_data, player_col='PLAYER', index_name='Player', select_cols='FPTS'))

	ranks = (pd.read_json(os.path.join(BASE_DIR, RANK_PATH, RANK_FILE))
			   .pipe(clean_data, player_col='Player (matchup)', index_name='Player',
	                 drop_cols=['Rank', 'FantasyProsAll Experts'], fill=''))

	# scoring
	scorer = scorer.fit(stats)
	scores = scorer.score(ranks)

	print scores
