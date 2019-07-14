import flask
import requests
import subprocess
import time
import threading
from flask_cors import CORS
app = flask.Flask(__name__)
CORS(app)
path_to_run = './'          #directory here
py_name = 'KM(Worker).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]

lrw=None
iplist=[]
s = "http://subworker"
sesh=requests.Session()


@app.route('/api/worker/start/<string:subworkers>', methods = ['GET'])
def start(subworkers):
    global lrw
    global sesh
    global iplist,s
    iplist = [s+str(i)+':4000' for i in range(0,int(subworkers))]
    if lrw is not None:    #if process is running or has run before
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run
        lrw=subprocess.Popen(args)
        time.sleep(1)
        for ip in iplist:
            url = ip+'/api/subworker/start/'+str(subworkers)
            initw = threading.Thread(target=sesh.get, args=(url,))
            initw.start()                   #start lr(worker) api
            time.sleep(1)
        return flask.Response(status=202)   #code:accepted

@app.route('/api/worker/stop', methods = ['GET'])
def stop():
    global lrw
    global sesh
    global iplist
    if lrw is not None:    #if process is running or has completed
        for ip in iplist:
            url = ip+'/api/subworker/stop'
            stopw = threading.Thread(target=sesh.get, args=(url,))
            stopw.start()
        lrw.terminate()
        lrw=None
        return flask.Response(status=200)   #code:ok
    else:                   #process never run
        return flask.Response(status=403)   #code:forbidden


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)
