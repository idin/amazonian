from s3fs import S3FileSystem
from pandas import read_csv, DataFrame
from pickle import dump as pickle_dump
from pickle import load as pickle_load
from psycopg2 import connect as psycopg2_connect
from csv import QUOTE_NONNUMERIC
from .S3File import S3Files


class S3:
	def __init__(self, key, secret, iam_role=None, path='s3://'):
		self._key = key
		self._secret = secret
		self._iam_role = iam_role
		if path is None:
			path = ''
		self._root = path

	@property
	def file_system(self):
		return S3FileSystem(key=self._key, secret=self._secret, use_ssl=False)

	def ls(self, path, detail=False, sort_by='path', sort_reverse=True, **kwargs):
		files = self.file_system.ls(path=path, detail=detail, **kwargs)
		if detail:
			files = S3Files(files)
			files.sort(by=sort_by, reverse=sort_reverse)
		else:
			if sort_by is not None:
				files.sort()
		return files

	def exists(self, path):
		return self.file_system.exists(path=path)

	def mkdir(self, path, **kwargs):
		return self.file_system.mkdir(path=path, **kwargs)

	def rm(self, path, **kwargs):
		return self.file_system.rm(path=path, **kwargs)

	def write(self, path, bytes):
		with self.file_system.open(path=self._root+path, mode='wb') as f:
			f.write(bytes)

	def write_csv(self, data, path, index=False, encoding='utf-8', **kwargs):
		"""

		:type data: DataFrame
		:param path:
		:param index:
		:param kwargs:
		:return:
		"""
		bytes = data.to_csv(path_or_buf=None, quoting=QUOTE_NONNUMERIC, index=index, **kwargs).encode(encoding)
		self.write(path=path, bytes=bytes)

	def read(self, path):
		with self.file_system.open(path=self._root+path,mode='rb') as f:
			result = f.read()
		return result


	def read_csv(self, path, encoding='utf-8', **kwargs):
		with self.file_system.open(self._root+path, 'rb', ) as f:
			df = read_csv(f, encoding=encoding, **kwargs)
		return df

	def write_pickle(self, obj, path):
		"""

		:type obj: DataFrame
		:param path:
		:return:
		"""
		with self.file_system.open(path=self._root+path, mode='wb') as f:
			try:
				obj.to_pickle(f)
			except:
				pickle_dump(obj=obj, file=f)

	def read_pickle(self, path):
		with self.file_system.open(path=self._root+path, mode='rb') as f:
			obj = pickle_load(file=f)
		return obj

	def copy_to_redshift(self, path, redshift, schema, table, truncate=False, create_table=False):
		if create_table:
			data = self.read_csv(path=path)
			redshift.create_table(data=data, name=table, schema=schema)

		connection = psycopg2_connect(f"""
			dbname='{redshift._database}' port='{redshift._port}' 
			user='{redshift._user_id}' password='{redshift._password}' 
			host='{redshift._server}'
		""")

		cursor = connection.cursor()

		if truncate:
			cursor.execute(f"TRUNCATE TABLE {schema}.{table}")

		if self._iam_role:
			credentials = f"IAM_ROLE '{self._iam_role}'"
		else:
			credentials = f"CREDENTIALS 'aws_access_key_id={self._key};aws_secret_access_key={self._secret}'"

		cursor.execute(f"""
			COPY {schema}.{table} FROM '{self._root+path}' 
			{credentials}
			FORMAT AS CSV ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1;commit;
		""")

		connection.close()
