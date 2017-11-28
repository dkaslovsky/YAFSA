""" TO BE DEPRECATED: Wrapper around metrics for scoring """

import numpy as np
import pandas as pd


def dcg(relevance_scores, k=None, numerator='rel', normalized=False):
	"""
	Discounted cumulative gain (see https://en.wikipedia.org/wiki/Discounted_cumulative_gain)
	:param relevance_scores: list or array of relevance/importance scores, ordered as returned by ranker being evaluated
	:param k: score only the first k elements of relevance_scores
	:param numerator: use relevance scores ('rel') or 2^relevance_scores - 1 ('exp') in numerator of dcg calculation
	:param normalized: boolean indicating whether to normalize by maximum possible dcg
	:return:
	"""

	# ensure k, if specified, is not longer than relevance_scores
	k = min(k, len(relevance_scores)) if k else len(relevance_scores)

	if numerator == 'rel':
		metric = sum(relevance_scores[:k] / np.log2(np.arange(2, k+2)))
	elif numerator == 'exp':
		metric = sum((np.power(2, relevance_scores[:k]) - 1) / np.log2(np.arange(2, k+2)))
	else:
		raise ValueError('Specify numerator as relevance_scores (\'rel\') or 2^relevance_scores - 1 (\'exp\')')

	if normalized:
		max_metric = dcg(sorted(relevance_scores, reverse=True), k=k, numerator=numerator, normalized=False)
		metric /= max_metric

	return metric


class Scorer(object):
	"""
	Evaluate rankings against ground truth values (points)
	"""

	def __init__(self, metric, normalize=True):
		self.metric = metric
		self.normalize = normalize
		# populated by fit
		self.points_ = None
		self.max_score_ = None

	def fit(self, points):
		"""
		Store ground truth points to be used to obtain true ordering
		:param points: Single column DataFrame or Series with points scored by each player
		:return:
		"""
		if isinstance(points, (pd.DataFrame, pd.Series)):
			self.points_ = pd.Series(points.values.ravel(), index=points.index, name='points')
		else:
			raise ValueError('points must be a Pandas Series or DataFrame with a single column')

		if self.normalize:
			# compute max possible score obtained by perfect ordering
			max_score = self.metric(sorted(self.points_.tolist(), reverse=True))
			if max_score > 0:
				self.max_score_ = max_score
			else:
				raise ValueError('normalization not possible with provided input')

		return self

	def score(self, ranks):
		"""
		Evaluate rankings against the ground truth ordering of points
		:param ranks: Series of projected rankings for each player or DataFrame with each column containing one ranking
		:return: Series of scores indexed by ranking
		"""
		if isinstance(ranks, pd.Series):
			return ranks.to_frame().apply(self._score)
		return ranks.apply(self._score)

	def _score(self, ranks):
		"""
		Evaluate single ranking
		:param ranks: Series of projected rankings for each player
		:return: score (float)
		"""
		if not isinstance(ranks, pd.Series):
			raise ValueError('ranks must be a Pandas Series')

		# join ranks and self.points_ on the index and sort by ranks
		df = (pd.concat([self.points_, ranks.rename('ranks')], axis=1)
				.dropna(axis=0, how='any')
				#.astype(int)
				.sort_values('ranks'))

		# compute (normalized) score
		scored = self.metric(df['points'].tolist())
		if self.normalize:
			scored /= self.max_score_

		return scored
