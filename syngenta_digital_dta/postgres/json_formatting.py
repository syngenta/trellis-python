from collections import OrderedDict

columns = OrderedDict(
    [
        ('guuid', 'generate_uuid_v4()'),
        ('created_at', 'now()'),
        ('updated_at', 'now()'),
    ]
)

json_columns = OrderedDict(
    [
        ('time', 'feature.properties.Time'),
        ('machine', 'feature.properties.Machine'),
        ('secID', 'feature.properties.SecID')
    ]
)

function_map = {
    'guuid': "cast({} as varchar)"
}

def insert_json_into_table(
        json,
        table_name,
        column_map,
        json_column_map,
        function_map={},
):
    [target_key] = set([v.split(".")[0] for v in json_column_map.values()])

    return (
        f"""{_build_json_cte(json)}
        {_build_insert_statement(table_name, column_map, json_column_map)}
        {_build_select_statement(column_map, json_columns, function_map)}
        FROM ({_build_json_array_subquery(target_key)}
"""
    )

def _build_json_cte(json):
    return f"WITH _json_cte AS (SELECT '{json}'::json AS _json)"


def _build_insert_statement(table, column_map, json_column_map):
    return f"INSERT INTO {table} ({', '.join(_get_column_order(column_map, json_column_map))})"


def _build_json_array_subquery(target_key):
    return f"SELECT json_array_elements(_json->'{target_key}') AS _jsondict FROM _json_cte"


def _build_select_statement(other_columns, json_columns, function_map):
    lines = []

    for alias, definition in other_columns.items():
        if alias in function_map:
            result = _apply_function(alias, definition, function_map)
            result = f"{result} AS {alias}"

        else:
            result = f"{definition} AS {alias}"

        lines.append(result)

    for alias, definition in json_columns.items():
        result = _parse_json_line(alias, definition)

        if alias in function_map:
            result = _apply_function(alias, result, function_map)

        lines.append(result)

    return f"SELECT {', '.join(lines)}"



def _get_column_order(column_map, json_column_map):
    return [
        *column_map.keys(),
        *json_column_map.keys()
    ]

def _parse_json_line(k, v):
    parts = v.split('.')

    if len(parts) == 2:
        statement = f"_jsondict -> '{parts[1]}' AS {k}"

    elif len(parts) == 3:
        statement = f"_jsondict -> '{parts[1]}' ->> '{parts[2]}' AS {k}"

    else:
        raise

    return statement

def _apply_function(k, statement, function_map):
    statement, alias = statement.split(' AS ')
    return f"{function_map[k].format(statement)} as {alias}"







