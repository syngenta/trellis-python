# Syngenta Digital DTA (Database Adapter)
A DRY multi-database normalizer.

## Features

  * Use the same package with multiple database engines
  * Able to validate your data against predefined schema in code (for no-schems solutions)
  * Creates easy pub-sub architecture based on model changes
  * Local development support

## Philosophy

The dta philosophy is to use one pattern with multiple databases

The dta encourages pub-sub architecture by allowing for the automatic publishing of data over SNS.

## Installation

This is a [python](https://www.python.org/) module available through the
[pypi registry](https://pypi.org).

Before installing, [download and install python](https://www.python.org/downloads/).
python 3 or higher is required.

Installation is done using the
[`pip install`](https://packaging.python.org/tutorials/installing-packages/) command:

```bash
$ pip install syngenta_digital_dta
```

or

```bash
$ pipenv install syngenta_digital_dta
```

## Common Usage: DynamoDB

```python
import os
import syngenta_digital_dta

adapter = syngenta_digital_dta.adapter(
    engine='dynamodb',
    table=os.getenv('DYNAMODB_TABLE'),
    endpoint='http://localhost:4000',
    model_schema='v1-table-model',
    model_schema_file='application/openapi.yml',
    model_identifier='test_id',
    model_version_key='modified'
)
```

**Initialize Options**

Option Name        | Required | Type   | Description
:-----------       | :------- | :----- | :----------
`engine`           | true     | string | name of supported db engine (dynamodb)
`table`            | true     | string | name of dynamodb table
`endpoint`         | false    | string | url of the dynamodb table (useful for local development)
`model_schema`     | true     | string | key of openapi schema this is being set against
`model_schema_file`| true     | string | path where your schema file can found (accepts JSON as well)
`model_identifier` | true     | string | unique identifier key on the model
`model_version_key`| true     | string | key that can be used as a version key (modified timestamps often suffice)
`author_identifier`| false    | string | unique identifier of the author who made the change (optional)
`sns_arn`          | false    | string | sns topic arn you want to broadcast the changes to

**Examples**

### DynamoDB Create

```python
result = adapter.create(
	operation='insert', # or overwrite (optional); defaults to insert
	data=some_dict_to_insert_into_the_table,
)

result = adapter.insert(data=some_dict_to_insert_into_the_table) # alias
```

### DynamoDB Read

```python
result = adapter.read(
    operation='get', # or query or scan (optional); defaults to get
	query={
	   'Key': {
	        'example_id': '3'
	   }
    }
)

result = adapter.get(
	query={
	   'Key': {
	        'example_id': '3'
	   }
    }
)
```

### DynamoDB Update

```python
result = adapter.update(
	data=some_dict_to_update_the_model,
	operation='get',
	query={
	   'Key': {
	        'example_id': '3'
	   }
    }
)
```

### DynamoDB Delete

```python
result = adapter.delete(
	query={
	   'Key': {
	        'example_id': '3'
	   }
    }
)
```

## Common Usage: Postgres & Redshift

```python
import os
import syngenta_digital_dta

adapter = syngenta_digital_dta.adapter(
    engine='postgres', # or redshift
    table='users',
    endpoint='localhost',
    database='dta',
    port=5439, # 5432 for redshift
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    model_schema='test-postgres-user-model',
    model_schema_file='tests/openapi.yml',
    model_identifier='user_id',
    model_version_key='modified',
    relationships={
        'addresses': 'user_id'
    }
)
```

**Initialize Options**

Option Name        | Required | Type   | Description
:-----------       | :------- | :----- | :----------
`engine`           | true     | string | name of supported db engine (dynamodb)
`table`            | true     | string | name of postgres table to work as primary query point
`endpoint`         | true     | string | url of the postgres cluster
`database`         | true     | string | name of the database to connect to
`port`             | true     | int    | port of database (defaults to 5439)
`user`             | true     | string | username for database access
`password`         | true     | string | password for database access
`model_schema`     | true     | string | key of openapi schema this is being set against
`model_schema_file`| true     | string | path where your schema file can found (accepts JSON as well)
`model_identifier` | true     | string | unique identifier key on the model
`model_version_key`| true     | string | key that can be used as a version key (modified timestamps often suffice)
`autocommit`       | false    | boolean| will commit transactions automatically without direct call
`relationships`    | false    | dict   | key is the table with the relationship and value is the foreign key on that table (assumes your primiary key name is equal to that table's foreign key)
`author_identifier`| false    | string | unique identifier of the author who made the change (optional)
`sns_arn`          | false    | string | sns topic arn you want to broadcast the changes to

**Examples**

### Postgres/Reshift Connect

```python
# will always pool and share connections
self.user_adapter.connect()
```

### Postgres/Reshift Create

```python
data = {
    'user_id': str(uuid.uuid4()),
    'email': 'somen.user@some-email.com',
    'first': 'Some',
    'last': 'User'
}
result = self.user_adapter.create(data=data, commit=True)
result = self.user_adapter.insert(data=data, commit=True) # alias
```

### Postgres/Reshift Update

```python
data = {
    'user_id': 'some-update-guid',
    'email': 'somen.user@some-email.com',
    'first': 'Some',
    'last': 'User'
}
result = self.user_adapter.update(data=data, commit=True)
```

### Postgres/Reshift Upsert

```python
data = {
    'user_id': 'some-update-guid',
    'email': 'somen.user@some-email.com',
    'first': 'Some',
    'last': 'User'
}
result = self.user_adapter.upsert(data=data, commit=True)
```

### Postgres/Reshift Delete

```python
self.user_adapter.delete('some-delete-guid', commit=True)
```

### Postgres/Reshift Read

```python
# will only return 1 row or None
result = self.user_adapter.read('some-read-guid')
result = self.user_adapter.get('some-read-guid') # alias

# all fields optional (defaults to SELECT * FROM {table} ORDER BY {model_identifier} ASC LIMIT 1000)
results = self.user_adapter.read_all(
    where={
        'first': 'first',
        'last': 'last',
    },
    limit=2,
    offset=1,
    orderby_column='first',
    orderby_sort='DESC'
)

# limited to get 1 relationship at a time
results = self.user_adapter.get_relationship('addresses', where={'user_id': 'some-user-relationship-guid'})

# only will allow read-only operations
# query and params are required; params can be empty dict
results = self.user_adapter.query(
    query='SELECT * FROM users WHERE user_id = %(identifier_value)s',
    params={
        'identifier_value':'some-query-relationship-guid'
    }
)
```

## Common Usage: Elasticsearch

```python
import os
import syngenta_digital_dta

adapter = syngenta_digital_dta.adapter(
    engine='elasticsearch',
    index='users',
    endpoint='localhost',
    model_schema='test-elasticsearch-user-model',
    model_schema_file='tests/openapi.yml',
    model_identifier='user_id',
    model_version_key='modified'
)
```

**Initialize Options**

Option Name        | Required | Type   | Description
:-----------       | :------- | :----- | :----------
`engine`           | true     | string | name of supported db engine (dynamodb)
`index`            | true     | string | name of postgres table to work as primary query point
`endpoint`         | true     | string | url of the postgres cluster
`port`             | false    | int    | port of database (defaults to 9200 if localhost or 443 if not)
`model_schema`     | true     | string | key of openapi schema this is being set against
`model_schema_file`| true     | string | path where your schema file can found (accepts JSON as well)
`model_identifier` | true     | string | unique identifier key on the model
`model_version_key`| true     | string | key that can be used as a version key (modified timestamps often suffice)
`author_identifier`| false    | string | unique identifier of the author who made the change (optional)
`sns_arn`          | false    | string | sns topic arn you want to broadcast the changes to


### Elasticsearch Connection

```python
# elasticsearch is auto-connected to a shared connection; use this to test that connection
self.adapter.connection.ping()
```

### Elasticsearch Set-up

```python
# will convert openapi schema, defined in init, to a mapping
self.adapter.create_template(
    name='users',
    index_patterns='users-*',
    special={'phone': 'keyword'} # (optional) can send mapping of special types otherwise will default based on schema type
)

# will use sensible defaults or you can pass in a custom settings kwargs['settings']
self.adapter.create_index(settings=some_optional_settings)
```

### Elasticsearch Create

```python
data = {
    'user_id': uuid.uuid4().hex,
    'email': 'somen.user@some-email.com',
    'first': 'Some',
    'last': 'User',
    'phone': 1112224444
}
self.adapter.create(data=data, refresh=True) # (optional) refresh defaults to True
```


### Elasticsearch Update

```python
updated_data = {
    'user_id': user_id,
    'email': 'peter.cruse@some-email.com',
    'first': 'Peter'
}
self.adapter.update(data=updated_data, refresh=True) # (optional) refresh defaults to True
```

### Elasticsearch Upsert

```python
data = {
    'user_id': upsert_id,
    'email': 'somen.user-upsert@some-email.com',
    'first': 'Some',
    'last': 'User',
    'phone': 1112224444
}
self.adapter.upsert(data=data, refresh=True) # (optional) refresh defaults to True
```

### Elasticsearch Delete

```python
self.adapter.delete(delete_id, refresh=True) # (optional) refresh defaults to True
```

### Elasticsearch Read

```python
response = self.adapter.get(get_id)

# returns single dictionary mapped to openapi model defined in init (or empty dict)
dict_response = self.adapter.get(get_id, normalize=True) # (optional) normalize defaults to False

# returns list of dictionaries mapped to openapi model defined in init (or empty array)
list_response = self.adapter.query(
    normalize=True, # (optional) normalize defaults to False
    query={
        'match': {
            'first': 'Normal'
        }
    }
)
```

## Contributing
If you would like to contribute please make sure to follow the established patterns and unit test your code:

### Unit Testing
To run unit test, enter command:
```bash
RUN_MODE=unittest python -m unittest discover
```
