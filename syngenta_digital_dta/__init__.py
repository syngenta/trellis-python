def adapter(**kwargs): # pylint: disable=R0911
    if kwargs.get('engine') == 'dynamodb':
        from syngenta_digital_dta.dynamodb.adapter import DynamodbAdapter  # pylint: disable=C
        return DynamodbAdapter(**kwargs)
    if kwargs.get('engine') == 'redshift':
        from syngenta_digital_dta.postgres.adapter import PostgresAdapter  # pylint: disable=C
        return PostgresAdapter(**kwargs)
    if kwargs.get('engine') == 'postgres':
        from syngenta_digital_dta.postgres.adapter import PostgresAdapter  # pylint: disable=C
        return PostgresAdapter(**kwargs)
    if kwargs.get('engine') == 'elasticsearch':
        from syngenta_digital_dta.elasticsearch.adapter import ElasticsearchAdapter  # pylint: disable=C
        return ElasticsearchAdapter(**kwargs)
    if kwargs.get('engine') == 's3':
        from syngenta_digital_dta.s3.adapter import S3Adapter  # pylint: disable=C
        return S3Adapter(**kwargs)
    if kwargs.get('engine') == 'file_system':
        from syngenta_digital_dta.file_system.adapter import FileSystemAdapter  # pylint: disable=C
        return FileSystemAdapter(**kwargs)
    if kwargs.get('engine') == 'mongo':
        from syngenta_digital_dta.mongo.adapter import MongoAdapter  # pylint: disable=C
        return MongoAdapter(**kwargs)
    raise Exception(
        f'engine {kwargs.get("engine", "was not supplied, an empty engine kwarg is")} not supported; contribute to get it supported :)')
