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
`sns_topic_arn`    | false    | string | sns topic arn you want to broadcast the changes to

**Examples**

```python
# create
result = adapter.create(
    operation='insert', # or overwrite (optional); defaults to insert
	data=some_dict_to_insert_into_the_table,
)

# insert
result = adapter.insert(
	data=some_dict_to_insert_into_the_table,
)

# get
result = adapter.read(
    operation='get', # or query or scan (optional); defaults to get
	query={
	   'Key': {
	        'example_id': '3'
	   }
    }
)

# get
result = adapter.get(
	query={
	   'Key': {
	        'example_id': '3'
	   }
    }
)

# update
result = adapter.update(
	data=some_dict_to_update_the_model,
	operation='get',
	query={
	   'Key': {
	        'example_id': '3'
	   }
    }
)

# delete
result = adapter.delete(
	query={
	   'Key': {
	        'example_id': '3'
	   }
    }
)
```

## Common Usage: Redshift

```python
import os
import syngenta_digital_dta

adapter = syngenta_digital_dta.adapter(
    engine='redshift',
    table='users',
    endpoint='localhost',
    database='dta',
    port=5439,
    user=os.getenv('REDSHIFT_USER'),
    password=os.getenv('REDSHIFT_PASSWORD'),
    model_schema='test-redshift-user-model',
    model_schema_file='tests/openapi.yml',
    model_identifier='user_id',
    model_version_key='modified',
    relationships={
        f'{ADDRESSES_TABLE}': 'user_id'
    }
)
```

**Initialize Options**

Option Name        | Required | Type   | Description
:-----------       | :------- | :----- | :----------
`engine`           | true     | string | name of supported db engine (dynamodb)
`table`            | true     | string | name of redshift table to work as primary query point
`endpoint`         | true     | string | url of the redshift cluster
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
`sns_topic_arn`    | false    | string | sns topic arn you want to broadcast the changes to

**Examples**

```python
# connect()
# will always pool and share connections
self.user_adapter.connect()

# create()
# alias for insert()
# will raise an error if not unique model identifier
# will return the data you sent
data = {
    'user_id': str(uuid.uuid4()),
    'email': 'paul.cruse@syngenta.com',
    'first': 'Paul',
    'last': 'Cruse III'
}
result = self.user_adapter.create(data=data, commit=True)

# update()
# will raise an error if not row doesn't exist, using the model identifier
# will return the data you sent
data = {
    'user_id': 'some-update-guid',
    'email': 'paul.cruse@syngenta.com',
    'first': 'Paul',
    'last': 'Cruse III'
}
result = self.user_adapter.update(data=data, commit=True)

# upsert()
# will overwrite anything that is already there
# will return the data you sent
data = {
    'user_id': 'some-update-guid',
    'email': 'paul.cruse@syngenta.com',
    'first': 'Paul',
    'last': 'Cruse III'
}
result = self.user_adapter.upsert(data=data, commit=True)

# delete()
# will raise an error if record doesn't exists
self.user_adapter.delete('some-delete-guid', commit=True)

# read()
# alias for get()
# will only return 1 row or None
result = self.user_adapter.read('some-read-guid')

# read_all()
# alias for get_all()
# will only return limit of 1000 rows or empty list []
# will auto sort by model identifier ASC
# will always return an array of dicts
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

# get_relationship()
# limited to get 1 relationship at a time
# will auto sort by model identifier ASC
# will always return an array of dicts
user_data = {
    'user_id': 'some-user-relationship-guid',
    'email': 'paul.cruse@syngenta.com',
    'first': 'Paul',
    'last': 'Cruse III'
}
address_data = {
    'address_id': 'some-address-guid',
    'user_id': 'some-user-relationship-guid',
    'address': '400 Street',
    'city': 'Chicago',
    'state': 'IL',
    'zipcode': '60616'
}
results = self.user_adapter.get_relationship('addresses', where={'user_id': 'some-user-relationship-guid'})

# query()
# WARNNING: USE LIGHTY; AVOID SQL IN THE CODEBASE
# REMEMBER: Redshift is Postgres Lite... what works in postgres may not in reshift
# only will allow read-only operations
# will always return an array of dicts
# query and params are required; params can be empty dict
results = self.user_adapter.query(
    query='SELECT * FROM users WHERE user_id = %(identifier_value)s',
    params={
        'identifier_value':'some-query-relationship-guid'
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
