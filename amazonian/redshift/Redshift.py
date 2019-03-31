from .BasicRedshift import BasicRedshift
from .Schema import Schema
from .Snapshot import Snapshot
from pandas import DataFrame


class Redshift(BasicRedshift):
	def __init__(self, user_id, password, server, database, port='5439', echo=0):
		super().__init__(user_id=user_id, password=password, port=port, server=server, database=database)
		self._schemas = None
		self._hierarchy = None
		self._table_data = None
		self._column_data = None
		self._echo = echo

	def __getstate__(self):
		state = super().__getstate__()
		state.refresh({'echo': self._echo})
		return state

	def __setstate__(self, state):
		super().__setstate__(state)
		self._echo = state['echo']
		self._schemas = None
		self._hierarchy = None
		self._table_data = None
		self._column_data = None

	@property
	def echo(self):
		return self._echo is not None and self._echo

	@echo.setter
	def echo(self, echo):
		self._echo = echo

	def _update_tables_data(self):
		self._table_data = self.get_tables_data(echo=self.echo)

	@property
	def table_data(self):
		"""
		:rtype: DataFrame
		"""
		if self._table_data is None:
			self._update_tables_data()
		return self._table_data

	shape = table_data

	def _update_columns_data(self):
		self._column_data = self.get_columns_data(echo=self.echo)

	@property
	def column_data(self):
		"""
		:rtype: DataFrame
		"""
		if self._column_data is None:
			self._update_columns_data()
		return self._column_data

	def _update_hierarchy(self):
		self._hierarchy = {
			schema: list(data['table'].unique()) for schema, data in self.table_data.groupby(by='schema')
		}

	@property
	def hierarchy(self):
		"""
		:rtype: dict[str,list[str]]
		"""
		if self._hierarchy is None:
			self._update_hierarchy()
		return self._hierarchy

	def _update_schemas(self):
		self._schemas = {schema: Schema(name=schema, redshift=self) for schema in self.get_schema_list()}

	@property
	def schemas(self):
		"""
		:rtype: dict[str,Schema]
		"""
		if self._schemas is None:
			self._update_schemas()
		return self._schemas

	@property
	def schema_list(self):
		"""
		:rtype: list[Schema]
		"""
		return [schema for _, schema in self.schemas.items()]

	def get_schema_list(self):
		return list(self.table_data['schema'].unique())

	def get_table(self, schema, table):
		return self.schemas[schema].tables[table]

	def __getitem__(self, item):
		return self.schemas[item]

	def refresh(self):
		self._update_tables_data()
		self._update_columns_data()
		self._update_hierarchy()
		self._update_schemas()

	def take_snapshot(self):
		return Snapshot(database=self)


