""" Driver script for analyzing rankings """


### THIS IS VERY MUCH WORK IN PROGRESS ###

import os
import pandas as pd

from functools import partial
from yafsa.clean import clean_data
from yafsa.score import DCGScorer


BASE_DIR = os.path.dirname(__file__)
STATS_PATH = os.path.join('data', 'stats')
RANK_PATH = os.path.join('data', 'rankings')

# args to loop over
YEAR = 2016
POSITIONS = ['QB', 'RB', 'WR', 'TE']
WEEKS = range(1, 18)
SOURCES = range(1, 5)

scorer = DCGScorer(k=30, numerator='rel', normalize=True)


if __name__ == '__main__':

	scores_by_position = {}

	for position in POSITIONS:

		dfs = []

		for week in WEEKS:

			# ETL stats
			stats_file = '%s_week=%s_pos=%s.json' % (YEAR, week, position)
			stats = (pd.read_json(os.path.join(BASE_DIR, STATS_PATH, stats_file))
			           .pipe(clean_data, player_col='PLAYER', index_name='Player', select_cols='FPTS'))

			# ETL ranks
			ranks_list = []
			for source in SOURCES:
				rank_file = '%s_week=%s_position=%s_source=%s.json' % (YEAR, week, position, source)
				ranks = (pd.read_json(os.path.join(BASE_DIR, RANK_PATH, rank_file))
				           .pipe(clean_data, player_col='Player (matchup)', index_name='Player',
				                 drop_cols=['Rank', 'FantasyProsAll Experts'], fill=''))
				ranks_list.append(ranks)
			ranks = pd.concat(ranks_list, axis=1)

			# scoring
			scorer = scorer.fit(stats)
			scores = scorer.score(ranks)
			dfs.append(scores.rename('Week %i' % week))

		scores_by_position[position] = pd.concat(dfs, axis=1)

	all_scores = pd.Panel(scores_by_position)

	# TODO: MAKE SURE TO CHECK THE INDEX OF EACH PANEL (all_scores['QB'].index -> does it contain only QBs?)

	import matplotlib
	matplotlib.use('tkagg')
	import matplotlib.pyplot as plt
	plt.ion()

	fig, ax = plt.subplots(nrows=1, ncols=all_scores.shape[0])
	for i, (position, scores) in enumerate(all_scores.iteritems()):
		ax[i].imshow(scores, aspect='auto', interpolation='nearest')
		ax[i].set_title(position)
		ax[i].set_xticks(range(scores.shape[1]))
		ax[i].set_xticklabels(scores.columns.map(lambda x: x.split(' ')[1]))
		ax[i].set_yticks(range(scores.shape[0]) if i==0 else [])
		ax[i].set_yticklabels(scores.index, visible=i==0)
		fig.subplots_adjust(hspace=0)
		fig.tight_layout()
