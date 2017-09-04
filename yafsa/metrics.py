""" ranking metrics """

import numpy as np


def dcg(relevance_scores, k=None, numerator='rel', normalized=True):
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
