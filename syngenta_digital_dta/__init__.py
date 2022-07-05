    def adapter(**kwargs):
    if kwargs.get('engine') == 'dynamodb':
        from syngenta_digital_dta.dynamodb.adapter import DynamodbAdapter
        return DynamodbAdapter(**kwargs)
    if kwargs.get('engine') == 'redshift':
        from syngenta_digital_dta.postgres.adapter import PostgresAdapter
        return PostgresAdapter(**kwargs)
    if kwargs.get('engine') == 'postgres':
        from syngenta_digital_dta.postgres.adapter import PostgresAdapter
        return PostgresAdapter(**kwargs)
    if kwargs.get('engine') == 'elasticsearch':
        from syngenta_digital_dta.elasticsearch.adapter import ElasticsearchAdapter
        return ElasticsearchAdapter(**kwargs)
    if kwargs.get('engine') == 's3':
        from syngenta_digital_dta.s3.adapter import S3Adapter
        return S3Adapter(**kwargs)
    if kwargs.get('engine') == 'file_system':
        from syngenta_digital_dta.file_system.adapter import FileSystemAdapter
        return FileSystemAdapter(**kwargs)
    raise Exception(
        f'engine {kwargs.get("engine", "was not supplied, an empty engine kwarg is")} not supported; contribute to get it supported :)')
