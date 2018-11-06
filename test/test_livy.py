import json
import pprint
import requests
import textwrap

livy_prefix = "127.0.0.1:8998"
data = {'kind': 'spark'}
headers = {"Content-Type": 'application/json'}
r = requests.post("http://{0}/batches".format(livy_prefix), data=json.dumps(data), headers=headers)
r.json()