from datetime import datetime
import json
import pytz
import re

from influxdb import InfluxDBClient

pattern = re.compile(r'(#?[\w-]*f+u+(cc|c+k|q|k\b|kk)[\w-]*)', re.IGNORECASE)
cleanup = re.compile(b"\x01|\x1f|\x02|\x12|\x0f|\x16|\x03(?:\d{1,2}(?:,\d{1,2})?)?")
usercheck = re.compile(r'^[\w-]+$')

client = InfluxDBClient(database='fucksgiven')
client.create_database('fucksgiven')

with open('nick_merge.json', 'r') as c:
        canon = json.load(c)

with open('irc.log', 'rb') as l:
    lines = l.readlines()

def get_timestamp(timestamp):
    if timestamp == 0:
        timestamp = 1444521600
    return datetime.fromtimestamp(timestamp, tz=pytz.utc).strftime('%Y-%m-%dT%H:%M:%S%Z')

def process_line(line):
    line = cleanup.sub(b'', line).decode('UTF-8', errors='replace')
    timestamp, user = line.split('\t')[:2]
    timestamp = get_timestamp(int(timestamp))
    sentence = ' '.join([i for i in line.split('\t')[2:]])
    sentence = ''.join([i + ' ' for i in sentence.split() if not re.match(r'^<?https?:\/\/', i)]).strip()
    if user in canon:
        user = canon[user]
    payload = {
        'measurement': 'fucks',
        'tags': {
            'user': user
        },
        'time': timestamp,
        'fields': {
            'sentence': sentence
        }
    }
    total = 0
    fucks = []
    if sentence and not sentence.startswith('!'):
        results = pattern.findall(sentence)
        if user in canon:
            user = canon[user]
        if results and usercheck.match(user):
            for result in results:
                if 'brainfuck' not in result[0].lower():
                    fucks.append(result[0])
                    total += 1
    payload['fields']['value'] = total
    payload['fields']['fucks'] = ','.join(fucks)
    print(payload['tags']['user'])
    return [payload]

if __name__=='__main__':
    for line in lines:
        try:
            client.write_points(process_line(line))
        except:
            continue

print(client.query('select value from fucks'))
