# Amazonian

*Amazonian* is a *Python* library for interacting easily with Amazon S3 and Redshift.

# Installation
`pip install amazonian`

# Usage

## `S3`

```python
from amazonian import S3

s3 = S3(key=None, secret=None, iam_role=None, root='s3://', spark=spark)

# get list of files:
s3.ls(path='some_path')

# get file size:
s3.get_size(path='some_file')

# save a Spark DataFrame as a Parquet
s3.save_parquet(data=my_data, path='some_path/name.parquet')

# load a Parquet into a Spark DataFrame
my_data = s3.load_parquet(path='some_path/name.parquet')
```