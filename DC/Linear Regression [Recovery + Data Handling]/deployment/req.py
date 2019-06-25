import json
import requests
import sys
ip = sys.argv[1]
call = sys.argv[2]
filename = sys.argv[3]
splits = sys.argv[4]
path = sys.argv[5]
header = sys.argv[6]
url = 'http://'+ip+"/"+call
payload = {'filename': filename,'splits':splits,'path':path,'header':header}
headers = {'content-type': 'application/json'}
print(payload)
r = requests.post(url, data=json.dumps(payload), headers=headers)
ret = r.content
print(ret.decode('ascii'))
