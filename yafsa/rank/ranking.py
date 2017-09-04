""" Meta wrapper around metrics for scoring """

import pandas as pd


class Ranker(object):
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
				.astype(int)
				.sort_values('ranks'))

		# compute (normalized) score
		scored = self.metric(df['points'].tolist())
		if self.normalize:
			scored /= self.max_score_

		return scored
