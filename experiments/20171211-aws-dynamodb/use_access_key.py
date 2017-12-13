import boto3
import random
import time
# from os import environ


# environ['AWS_ACCESS_KEY_ID'] = 'abc'
# environ['AWS_SECRET_ACCESS_KEY'] = 'xyz'
dynamodb = boto3.resource('dynamodb')
table_name = 'human-trafficking-usa-doj-20171211-1429'
table = dynamodb.Table(table_name)
print('table_name = %s' % table_name)
print('item_count = %s' % table.item_count)


def cycle_items(f, d):
    while True:
        response = f(d)
        for item in response['Items']:
            yield(item)
        try:
            d['ExclusiveStartKey'] = response['LastEvaluatedKey']
        except KeyError:
            break
        time.sleep(1)


# Print titles for first 500 items
for x in cycle_items(table.scan, {
        'AttributesToGet': ['uuid'], 'Limit': 500}):
    print('title = %s' % x['title'].replace('\n', ' '))


# Get first 500 uuids
uuids = [x['uuid'] for x in cycle_items(table.scan, {
    'AttributesToGet': ['uuid'], 'Limit': 500})]
# assert len(uuids) == table.item_count


# Get a specific item
response = table.get_item(
    Key={'uuid': random.choice(uuids)}
)
item = response['Item']
print('uuid = %s' % item['uuid'])
print('title = %s' % item['title'])
