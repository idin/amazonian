class Column:
	def __init__(self, name, table, echo=None):
		self._name = name
		self._table = table
		if echo is None:
			self._echo = self.table.echo
		else:
			self._echo = echo

	@property
	def name(self):
		return self._name

	@property
	def echo(self):
		return self._echo is not None and self._echo

	@echo.setter
	def echo(self, echo):
		self._echo = echo

	@property
	def table(self):
		return self._table

	@property
	def value_counts(self):
		result = self.table.schema.database.get_dataframe(
			echo=self.echo,
			query=(
				f'SELECT \'{self.table.schema.name}\' AS "schema", \'{self.table.name}\' AS "table", '
				f'\'{self.name}\' AS "column", "{self.name}" AS "value", ' 
				f'COUNT(*) AS "count" FROM {self.table.schema.name}.{self.table.name} ' 
				f'GROUP BY "{self.name}" ORDER BY "count" DESC '
			)
		)
		#result['ratio'] = result['count'] / self.table.num_rows
		return result

	def __str__(self):
		return f'"{self.name}" column'

	def __repr__(self):
		return str(self)