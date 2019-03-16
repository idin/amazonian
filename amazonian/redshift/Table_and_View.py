from .Column import Column

class _TableOrView:
	def __init__(self, name, schema, _type, echo=None):
		"""
		:type name: str
		:type schema: Schema
		:type echo: int or NoneType
		"""
		self._name = name
		self._schema = schema
		self._type = _type
		self._data = None
		self._columns_info = None
		self._columns = None
		self._num_rows = None
		if _type == 'view':
			if name not in schema.view_names: raise KeyError(f'{_type} "{name}" not in "{schema}"')
		else:
			if name not in schema.table_names: raise KeyError(f'{_type} "{name}" not in "{schema}"')
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
			self._data = self.schema._redshift.get_dataframe(query=query, echo=self.echo)
		return self._data

	def get_head(self, num_rows=5):
		query = f'SELECT TOP {num_rows} * FROM ' + self.schema.name + '.' + self.name
		return self.schema._redshift.get_dataframe(query=query, echo=self.echo)

	@property
	def columns_info(self):
		if self._columns_info is None:
			columns_data = self.schema.database.columns_data
			self._columns_info = columns_data[(columns_data['schema']==self.schema.name) & (columns_data['table']==self.name)].copy()
		return self._columns_info

	@property
	def column_names(self):
		"""
		:rtype: list of str
		"""
		return list(self.columns_info['column'].values)

	@property
	def columns(self):
		if self._columns is None:
			self._columns = {column_name:Column(name=column_name, table=self) for column_name in self.column_names}
		return self._columns

	@property
	def num_rows(self):
		try:
			return self.shape['num_rows'].values[0]
		except:
			if self._num_rows is None:
				self._num_rows = self.schema.database.get_dataframe(
					query=f'SELECT COUNT(*) as "count" FROM {self.schema.name}.{self.name}', echo=self.echo
				)['count'].values[0]
			raise
			return self._num_rows

	@property
	def shape(self):
		return self.schema.shape[self.schema.shape['table']==self.name]

	def __str__(self):
		return f'{str(self.schema)}.{self.name} ({self._type})'

	def __repr__(self):
		return str(self)


class Table(_TableOrView):
	def __init__(self, name, schema, echo=None):
		super().__init__(name=name, schema=schema, _type='table', echo=echo)


class View(_TableOrView):
	def __init__(self, name, schema, echo=None):
		super().__init__(name=name, schema=schema, _type='view', echo=echo)







