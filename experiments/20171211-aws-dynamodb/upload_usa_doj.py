import boto3
import json
import sys
import time
from os.path import basename, splitext


source_path = sys.argv[1]
# source_path = 'human-trafficking-usa-doj-20171111-1730.json'
table_name = splitext(basename(source_path))[0]
print('table_name = %s' % table_name)

dynamodb = boto3.resource('dynamodb')
try:
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{
            'AttributeName': 'uuid',
            'KeyType': 'HASH'
        }],
        AttributeDefinitions=[{
            'AttributeName': 'uuid',
            'AttributeType': 'S'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
except dynamodb.exceptions.ResourceInUseException:
    table = dynamodb.Table(table_name)
print(table.item_count)


def remove_empty_values(value_by_key):
    # AWS DynamoDB does not like empty values
    d = {}
    for k, v in value_by_key.items():
        if isinstance(v, dict):
            v = remove_empty_values(v)
        if isinstance(v, list):
            v = [remove_empty_values(x) for x in v]
        elif v == '':
            continue
        d[k] = v
    return d


with table.batch_writer() as batch:
    for index, line in enumerate(open(source_path)):
        d = json.loads(line)
        print(index, d['uuid'])
        batch.put_item(Item=remove_empty_values(d))
print(table.item_count)
