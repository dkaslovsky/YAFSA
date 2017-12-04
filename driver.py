""" Driver script for analyzing rankings """

import os
import pandas as pd

from functools import partial
from yafsa.clean import clean_data
from yafsa.score import DCGScorer, DifferenceScorer


BASE_DIR = os.path.dirname(__file__)
STATS_PATH = os.path.join('data', 'stats')
RANK_PATH = os.path.join('data', 'rankings')

# file specifications
YEAR = 2016
SOURCES = range(1, 5)
WEEKS = range(1, 18)
POSITIONS = ['QB', 'RB', 'WR', 'TE']

# define scorers
RANK_DEPTH_TO_ANALYZE = 25
dcg_scorer = DCGScorer(k=RANK_DEPTH_TO_ANALYZE, numerator='exp', normalize=True)
diff_scorer = DifferenceScorer(k=RANK_DEPTH_TO_ANALYZE, standardize=True)


def get_stats(year, week, position):
    """
    Read file specified by year, week, position, and return a series of scores
    :param year:
    :param week:
    :param position:
    :return:
    """
    stats_file = '%s_week=%s_pos=%s.json' % (YEAR, week, position)
    stats = (pd.read_json(os.path.join(BASE_DIR, STATS_PATH, stats_file))
               .pipe(clean_data, player_col='PLAYER', index_name='Player', select_cols='FPTS'))
    stats = stats['FPTS'].rename('Week %i' % week)
    return stats

def get_ranks(year, week, position, sources):
    """
    Read files specified by year, week, position, and sources and concatenate results to return dataframe
    :param year:
    :param week:
    :param position:
    :param sources:
    :return:
    """
    ranks_list = []
    for source in SOURCES:
        rank_file = '%s_week=%s_position=%s_source=%s.json' % (YEAR, week, position, source)
        ranks = (pd.read_json(os.path.join(BASE_DIR, RANK_PATH, rank_file))
                   .pipe(clean_data, player_col='Player (matchup)', index_name='Player',
                         drop_cols=['Rank', 'FantasyProsAll Experts'], fill=''))
        ranks_list.append(ranks)
    return pd.concat(ranks_list, axis=1)


if __name__ == '__main__':

    position = POSITIONS[2]  # WRs
    week_to_score = 1

    stats_by_week = pd.concat([get_stats(YEAR, week, position) for week in WEEKS], axis=1)

    # fit scorers
    dcg_scorer = dcg_scorer.fit(stats_by_week['Week %i' % week_to_score])
    diff_scorer = diff_scorer.fit(stats_by_week)

    # score the rankings
    ranks = get_ranks(YEAR, week_to_score, position, SOURCES)
    dcg_scores = dcg_scorer.score(ranks)
    diff_scores = diff_scorer.score(ranks, stats_by_week['Week %i' % week_to_score])

    print dcg_scores
    print diff_scores
