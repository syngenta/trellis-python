import uuid


def get_standard():
    return {
        'test_id': str(uuid.uuid4()),
        'test_query_id': str(uuid.uuid4()),
        'object_key': {
            'string_key': 'nothing'
        },
        'array_number': [1, 2, 3],
        'array_objects': [
            {
                'array_string_key': 'a',
                'array_number_key': 1
            }
        ],
        'created': '2020-10-05',
        'modified': '2020-10-05'
    }


def get_items():
    return [
        {
            'test_id': str(uuid.uuid4()),
            'test_query_id': str(uuid.uuid4()),
            'object_key': {
                'string_key': 'nothing'
            },
            'array_number': [1, 2, 3],
            'array_objects': [
                {
                    'array_string_key': 'a',
                    'array_number_key': 1
                }
            ],
            'created': '2020-10-05',
            'modified': '2020-10-05'
        },
        {
            'test_id': str(uuid.uuid4()),
            'test_query_id': str(uuid.uuid4()),
            'object_key': {
                'string_key': 'nothing'
            },
            'array_number': [1, 2, 3],
            'array_objects': [
                {
                    'array_string_key': 'a',
                    'array_number_key': 1
                }
            ],
            'created': '2020-10-05',
            'modified': '2020-10-05'
        }
    ]
