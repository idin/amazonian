from .BasicRedshift import BasicRedshift
from .Table_and_View import Table

class Schema:
	def __init__(self, name, redshift, echo=None):
		"""
		:type name: str
		:type redshift: BasicRedshift
		:type echo: int or NoneType
		"""
		self._name = name
		self._redshift = redshift
		self._tables = None
		self._views = None
		if echo is None:
			self._echo = max(0, self.database.echo-1)
		else:
			self._echo = echo
		if name not in redshift.get_schema_list(): raise KeyError('schema "{name}" not in database!')

	@property
	def echo(self):
		return self._echo is not None and self._echo

	@echo.setter
	def echo(self, echo):
		self._echo = echo

	@property
	def database(self):
		"""
		:rtype: .Redshift.Redshift
		"""
		return self._redshift

	@property
	def table_names(self):
		try:
			return self.database.hierarchy[self._name]#['tables']
		except:
			return []

	# @property
	# def view_names(self):
	# 	try:
	# 		return self.database.hierarchy[self._name]#['views']
	# 	except:
	# 		return []

	@property
	def tables(self):
		if self._tables is None:
			self._tables = {name:Table(name=name, schema=self) for name in self.table_names}
		return self._tables

	@property
	def table_list(self):
		return [table for _, table in self.tables.items()]

	@property
	def name(self):
		return self._name

	@property
	def shape(self):
		return self.database.shape[self.database.shape['schema']==self.name]

	def __str__(self):
		return f'{str(self.database)}.{self.name}'

	def __repr__(self):
		return str(self)

	def __getitem__(self, item):
		if item in self.tables:
			return self.tables[item]
		else:
			return self.views[item]



