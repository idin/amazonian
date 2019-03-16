from .BasicRedshift import BasicRedshift
from .Schema import Schema


class Redshift(BasicRedshift):
	def __init__(self, user_id, password, server, database, port='5439', echo=0):
		super().__init__(user_id=user_id, password=password, port=port, server=server, database=database)
		self._schemas = None
		self._hierarchy = None
		self._tables_data = None
		self._columns_data = None
		self._echo = echo

	@property
	def echo(self):
		return self._echo is not None and self._echo

	@echo.setter
	def echo(self, echo):
		self._echo = echo

	@property
	def tables_data(self):
		if self._tables_data is None:
			self._tables_data = self.get_tables_data(echo=self.echo)
		return self._tables_data

	shape = tables_data

	@property
	def columns_data(self):
		if self._columns_data is None:
			self._columns_data = self.get_columns_data(echo=self.echo)
		return self._columns_data

	@property
	def hierarchy(self):
		if self._hierarchy is None:
			self._hierarchy = {
				schema: list(data['table'].unique()) for schema, data in self.tables_data.groupby(by='schema')
			}
		return self._hierarchy

	@property
	def schemas(self):
		if self._schemas is None:
			self._schemas = {schema:Schema(name=schema, redshift=self) for schema in self.get_schema_list()}
		return self._schemas

	@property
	def schema_list(self):
		return [schema for _, schema in self.schemas.items()]

	def get_schema_list(self):
		return list(self.tables_data['schema'].unique())

	def get_table(self, schema, table):
		return self.schemas[schema].tables[table]

	def __getitem__(self, item):
		return self.schemas[item]

