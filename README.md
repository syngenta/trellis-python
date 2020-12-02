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

## Contributing
If you would like to contribute please make sure to follow the established patterns and unit test your code:

### Unit Testing
To run unit test, enter command:
```bash
RUN_MODE=unittest python -m unittest discover
```
