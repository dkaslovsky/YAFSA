""" Ranking metrics """

import numpy as np
import pandas as pd


class BaseScorer(object):

    """ Base class for scoring """

    @staticmethod
    def check_input(input_data, valid_types, max_columns=None):
        """
        Validate input
        :param input_data:
        :param valid_types: type or tuple of types to check for
        :param max_columns: maximum number of columns for tabular input_data
        :return:
        """
        valid_types = valid_types if isinstance(valid_types, tuple) else tuple([valid_types])
        if not isinstance(input_data, valid_types):
            raise TypeError('Invalid input type')
        if max_columns:
            if len(input_data.shape) == 2 and input_data.shape[1] > max_columns:
                raise ValueError('Input must have at most %i columns' % max_columns)

    def score(self, ranks, points=None):
        """
        Score projected ranks based on class metric
        :param ranks: DataFrame or Series of ranks for a position indexed by "expert ranker"
        :param points: Series of points scored for a position
        :return:
        """
        self.check_input(ranks, (pd.Series, pd.DataFrame))
        if isinstance(ranks, pd.Series):
            return ranks.to_frame().apply(self.metric, points=points)
        return ranks.apply(self.metric, points=points)

    @staticmethod
    def sort_points_by_rank(ranks, points):
        """
        Sort points in order of projected rankings
        :param ranks: Series of projected rankings indexed by player
        :param points: Series of actual points scored indexed by player
        :return:
        """
        rank_idx = ranks.sort_values().index
        return points.loc[rank_idx].dropna()


class DCGScorer(BaseScorer):

    """ Discounted cumulative gain (see https://en.wikipedia.org/wiki/Discounted_cumulative_gain) """

    def __init__(self, k=None, numerator='rel', normalize=True):
        """

        :param k: score only the first k rankings
        :param numerator: use relevance scores ('rel') or 2^relevance_scores - 1 ('exp') in numerator of dcg calculation
        :param normalize: boolean indicating whether to normalize by maximum possible dcg
        """
        self.k = k if k else np.inf
        self.normalize = normalize
        self.numerator_func = {
            'rel': self._rel_numerator,
            'exp': self._exp_numerator
        }.get(numerator)
        if not self.numerator_func:
            raise ValueError('Specify numerator as relevance_scores (\'rel\') or 2^relevance_scores-1 (\'exp\')')

        # populated by fit
        self.points_ = None
        self.max_score_ = None

    def fit(self, points):
        """
        Store ground truth points to be used to obtain true ordering
        :param points: Single column DataFrame or Series with points scored by each player
        :return:
        """
        self.check_input(points, (pd.DataFrame, pd.Series), max_columns=1)

        # format input to a pd.Series with name 'points'
        # (this implementation handles both Series and DataFrame input)
        self.points_ = pd.Series(points.values.ravel(), index=points.index, name='points')

        if self.normalize:
            # compute max possible score obtained by perfect ordering
            sorted_idx = self.points_.sort_values(ascending=False).index
            optimal_ranks = pd.Series(np.arange(len(sorted_idx)) + 1, index=sorted_idx)
            max_score = self._metric(optimal_ranks, normalize=False)
            if max_score <= 0:
                raise ValueError('Normalization not possible with provided input')
            self.max_score_ = max_score
        return self

    def metric(self, ranks, points=None):
        """

        :param ranks: Series of projected rankings indexed by player
        :param points: not used, included to conform to BaseScorer API
        :return:
        """
        return self._metric(ranks, self.normalize)

    def _metric(self, ranks, normalize):
        """
        Computes DCG
        :param ranks: Series of projected rankings indexed by player
        :param normalize: boolean
        :return:
        """
        # sort points by rank
        points_by_rank = self.sort_points_by_rank(ranks, self.points_)
        # ensure k does not exceed length of points_by_rank
        k = min(self.k, points_by_rank.shape[0])
        # DCG calculation
        numerator = self.numerator_func(points_by_rank.iloc[:k])
        denominator = np.log2(np.arange(2, k+2))
        score = sum(numerator / denominator)
        if normalize:
            score /= self.max_score_
        return score

    @staticmethod
    def _rel_numerator(x):
        return x

    @staticmethod
    def _exp_numerator(x):
        return np.power(2, x) - 1


class DifferenceScorer(BaseScorer):

    """ Score based on difference from mean points for a given rank """

    def __init__(self, k=None, standardize=False):
        """

        :param k: score only the first k rankings
        :param standardize: return standardized z-scores
        """
        self.k = k if k else np.inf
        self.standardize = standardize
        # populated by fit
        self.n_fitted_points_ = None
        self.points_mean_by_rank_ = None
        self.points_std_by_rank_ = None

    def fit(self, points_by_week):
        """
        Compute statistics from historical points
        :param points_by_week: DataFrame of weekly points
        :return:
        """
        self.check_input(points_by_week, pd.DataFrame)
        # sort each column's (week's) points
        sorted_points = -np.sort(-points_by_week.values, axis=0)
        sorted_points = sorted_points[~np.all(np.isnan(sorted_points), axis=1)]
        self.n_fitted_points_ = sorted_points.shape[0]
        # compute mean for each rank
        self.points_mean_by_rank_ = np.nan_to_num(np.nanmean(sorted_points, axis=1))
        if self.standardize:
            # standard deviation for each rank
            self.points_std_by_rank_ = np.nan_to_num(np.nanstd(sorted_points, axis=1))
            # replace zeros with the median std to avoid division by zero
            zero_fill_val = np.median(self.points_std_by_rank_[self.points_std_by_rank_ > 0])
            self.points_std_by_rank_[self.points_std_by_rank_ == 0] = zero_fill_val
        return self

    def metric(self, ranks, points):
        """
        Compute score as the average of the difference of the underperforming ranks
        :param ranks: Series of projected rankings indexed by player
        :param points: Series of actual points scored indexed by player
        :return:
        """
        points_by_rank = self.sort_points_by_rank(ranks, points)
        max_idx = min(points_by_rank.shape[0], self.n_fitted_points_, self.k)
        # compute difference between points scored by ith ranked player and the
        # average points scored by ith ranked player
        point_differences = points_by_rank[:max_idx] - self.points_mean_by_rank_[:max_idx]
        if self.standardize:
            point_differences /= self.points_std_by_rank_[:max_idx]
        # return aggregated point difference of underperforming ranks
        return np.average(point_differences, weights=point_differences < 0)
