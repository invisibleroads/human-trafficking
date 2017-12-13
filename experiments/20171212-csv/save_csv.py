# JSON = 412mb
# CSV = 332mb


import csv
import json
import sys
from os.path import splitext


def get_time(d, k):
    v = d.get(k)
    if not v:
        return ''
    return int(v)


def get_text(d, k):
    x = d.get(k, '') or ''
    return x.strip()


source_path = sys.argv[1]
target_path = splitext(source_path)[0] + '.csv'
csv_writer = csv.writer(open(target_path, 'wt'))
csv_writer.writerow([
    'uuid',
    'url',
    'topic_names',
    'component_names',
    'created_time',
    'changed_time',
    'published_time',
    'teaser',
    'title',
    'body',
])
for index, line in enumerate(open(source_path)):
    d = json.loads(line)
    uuid = d['uuid']
    print(index, uuid)
    csv_writer.writerow([
        uuid,
        get_text(d, 'url'),
        '; '.join(x['name'] for x in d['topic']),
        '; '.join(x['name'] for x in d['component']),
        get_time(d, 'created'),
        get_time(d, 'changed'),
        get_time(d, 'date'),
        get_text(d, 'teaser'),
        get_text(d, 'title'),
        get_text(d, 'body'),
    ])
