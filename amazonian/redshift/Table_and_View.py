from .Column import Column


class Table:
	def __init__(self, name, schema, echo=None):
		"""
		:type name: str
		:type schema: Schema
		:type echo: int or NoneType
		"""
		self._name = name
		self._schema = schema
		self._data = None
		self._columns_info = None
		self._columns = None
		self._num_rows = None
		self._dictionary = None
		if name not in schema.table_names:
			raise KeyError(f'"{name}" not in "{schema}"')
		if echo is None:
			self._echo = max(0, self.schema.echo-1)
		else:
			self._echo = echo

	@property
	def echo(self):
		return self._echo is not None and self._echo

	@echo.setter
	def echo(self, echo):
		self._echo = echo

	@property
	def schema(self):
		return self._schema

	@property
	def name(self):
		return self._name

	@property
	def data(self):
		if self._data is None:
			query = 'SELECT * FROM ' + self.schema.name + '.' + self.name
			self._data = self.schema.database.get_dataframe(query=query, echo=self.echo)
		return self._data

	def get_head(self, num_rows=5):
		query = f'SELECT TOP {num_rows} * FROM ' + self.schema.name + '.' + self.name
		return self.schema._redshift.get_dataframe(query=query, echo=self.echo)

	@property
	def columns_info(self):
		if self._columns_info is None:
			columns_data = self.schema.database.column_data
			self._columns_info = columns_data[
				(columns_data['schema'] == self.schema.name) & (columns_data['table'] == self.name)
			].copy()
		return self._columns_info

	@property
	def column_names(self):
		"""
		:rtype: list of str
		"""
		return list(self.columns_info['column'].values)

	@property
	def columns(self):
		"""
		:rtype: dict[str,Column]
		"""
		if self._columns is None:
			self._columns = {column_name: Column(name=column_name, table=self) for column_name in self.column_names}
		return self._columns.copy()

	@property
	def dictionary(self):
		"""
		:rtype: dict
		"""
		if self._dictionary is None:
			self._dictionary = self.schema.shape[self.schema.shape['table'] == self.name].iloc[0].to_dict()
		return self._dictionary.copy()

	@property
	def num_rows(self):
		return self.dictionary['num_rows']

	@property
	def num_columns(self):
		return self.dictionary['num_columns']

	@property
	def shape(self):
		return self.schema.shape[self.schema.shape['table'] == self.name]

	def __str__(self):
		return f'{str(self.schema)}.{self.name}'

	def __repr__(self):
		return str(self)
