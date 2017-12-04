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
    return stats['FPTS'].rename('Week %i' % week)


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


def scores_to_ranks(series):
    """
    Order rank a series of scores
    :param series:
    :return:
    """
    return series.shape[0] - series.sort_values(ascending=False).argsort()


if __name__ == '__main__':

    position = POSITIONS[2]  # just score WR rankings for now
    week_to_score = 1        # just score week 1 for now

    # get stats and ranks
    stats_by_week = pd.concat([get_stats(YEAR, week, position) for week in WEEKS], axis=1)
    ranks = get_ranks(YEAR, week_to_score, position, SOURCES)

    # fit scorers
    dcg_scorer = dcg_scorer.fit(stats_by_week['Week %i' % week_to_score])
    diff_scorer = diff_scorer.fit(stats_by_week)

    # scoring
    dcg_scores = dcg_scorer.score(ranks)
    diff_scores = diff_scorer.score(ranks, stats_by_week['Week %i' % week_to_score])

    order_rankings = pd.concat([
        scores_to_ranks(dcg_scores).rename('DCG'),
        scores_to_ranks(diff_scores).rename('Diff')
    ], axis=1)
    order_rankings['Composite'] = order_rankings.apply(sum, axis=1)
    print order_rankings.sort_values('Composite')
