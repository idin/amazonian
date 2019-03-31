from datetime import datetime
from pandas import DataFrame, concat


class Snapshot:
	def __init__(self, database):
		"""
		:type database: .Redshift.Redshift
		"""
		database.refresh()
		self._time = datetime.now()
		self._table_data = database.table_data.copy()
		self._column_data = database.column_data.copy()

	@property
	def time(self):
		"""
		:rtype: datetime
		"""
		return self._time

	@property
	def table_data(self):
		"""
		:rtype: DataFrame
		"""
		return self._table_data

	@property
	def column_data(self):
		"""
		:rtype: DataFrame
		"""
		return self._column_data

	def __getstate__(self):
		return {
			'time': self.time,
			'table_data': self.table_data,
			'column_data': self.column_data
		}

	def __setstate__(self, state):
		self._time = state['time']
		self._table_data = state['table_data']
		self._column_data = state['column_data']

	def __sub__(self, other):
		"""
		:type other: Snapshot
		:rtype SnapshotDifference
		"""
		return SnapshotDifference(snapshot1=self, snapshot2=other)


class SnapshotDifference:
	def __init__(self, snapshot1, snapshot2):
		"""
		:type snapshot1: Snapshot
		:type snapshot2: Snapshot
		"""

		column_comparison_cols = ['schema', 'table', 'column']
		columns1 = snapshot1.column_data[column_comparison_cols]
		columns2 = snapshot2.column_data[column_comparison_cols]
		self._column_comparisons = columns1.merge(
			right=columns2, on=column_comparison_cols, how='outer', indicator='indicator', validate='one_to_one'
		)
		self._column_comparisons['indicator'].replace(
			to_replace={'left_only': 'missing_column', 'right_only': 'new_column', 'both': 'same_column'},
			inplace=True
		)

		self.new_columns = self._column_comparisons[self._column_comparisons['indicator'] == 'new_column']
		self.missing_columns = self._column_comparisons[self._column_comparisons['indicator'] == 'missing_column']

		#
		#
		#
		#

		table_comp_same = ['schema', 'table']
		table_comparison_different_cols = ['table_id', 'num_rows', 'num_columns', 'mbytes']
		tables1 = snapshot1.table_data[table_comp_same + table_comparison_different_cols]
		tables2 = snapshot2.table_data[table_comp_same + table_comparison_different_cols]
		self._table_comparisons = tables1.merge(
			right=tables2, on=table_comp_same, how='outer', indicator='indicator', validate='one_to_one',
			suffixes=['_1', '_2']
		)
		for col in table_comparison_different_cols:

			if col == 'table_id':
				self._table_comparisons[f'{col}_1'] = self._table_comparisons[f'{col}_1'].fillna(-1).astype(int)
				self._table_comparisons[f'{col}_2'] = self._table_comparisons[f'{col}_2'].fillna(-1).astype(int)
				self._table_comparisons[f'{col}_different'] = \
					self._table_comparisons[f'{col}_1'] != self._table_comparisons[f'{col}_2']
			else:
				self._table_comparisons[f'{col}_1'] = self._table_comparisons[f'{col}_1'].fillna(0).astype(int)
				self._table_comparisons[f'{col}_2'] = self._table_comparisons[f'{col}_2'].fillna(0).astype(int)
				self._table_comparisons[f'{col}_difference'] = \
					self._table_comparisons[f'{col}_2'] - self._table_comparisons[f'{col}_1']

		self._table_comparisons['indicator'].replace(
			to_replace={'left_only': 'missing_table', 'right_only': 'new_table', 'both': 'same_table'}, inplace=True
		)

		self.table_growth = self._table_comparisons[
			(self._table_comparisons['indicator'] == 'same_table') & (
				(self._table_comparisons['num_rows_difference'] != 0) |
				(self._table_comparisons['num_columns_difference'] != 0) |
				(self._table_comparisons['mbytes_difference'] != 0) |
				(self._table_comparisons['table_id_different'])
			)
		]

		column_changes = self._column_comparisons.copy()
		column_changes['num_new_columns'] = (column_changes['indicator'] == 'new_column') * 1
		column_changes['num_missing_columns'] = (column_changes['indicator'] == 'missing_column') * 1
		column_changes = column_changes[['schema', 'table', 'num_new_columns', 'num_missing_columns']].groupby(
			['schema', 'table']
		).sum().reset_index(drop=False)
		table_changes = self._table_comparisons[['schema', 'table', 'indicator']]
		table_changes = table_changes.merge(
			right=column_changes, on=['schema', 'table'], how='outer', validate='one_to_one'
		)
		self.table_changes = table_changes[
			(table_changes['indicator'] != 'same_table') |
			(table_changes['num_new_columns'] > 0) |
			(table_changes['num_missing_columns'] > 0)
		]





