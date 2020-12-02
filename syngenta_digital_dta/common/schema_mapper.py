import simplejson as json
import jsonref
import yaml


def map_to_schema(data, schema_file, schema_key):
    model_data = {}
    model_schema = _get_model_schema(schema_file, schema_key)
    schemas = model_schema['allOf'] if model_schema.get('allOf') else [model_schema]
    for model in schemas:
        if model.get('type') == 'object':
            _populate_model_data(model.get('properties', {}), data, model_data)
    return model_data


def _get_model_schema(schema_file, schema_key):
    with open(schema_file) as openapi:
        api_doc = yaml.load(openapi, Loader=yaml.FullLoader)
    return jsonref.loads(json.dumps(api_doc))['components']['schemas'][schema_key]


def _populate_model_data(properties, data, model_data):
    for property_key, property_value in properties.items():
        model_data[property_key] = {}
        if property_value.get('properties'):
            _populate_model_data(property_value['properties'], data.get(property_key), model_data[property_key])
        elif property_value.get('items', {}).get('properties'):
            model_data[property_key] = []
            for index in range(len(data.get(property_key, []))):
                pop = _populate_model_data(property_value['items']['properties'], data[property_key][index], {})
                model_data[property_key].append(pop)
        else:
            model_data[property_key] = data.get(property_key)
    return model_data
