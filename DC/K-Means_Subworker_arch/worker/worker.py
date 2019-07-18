import flask
import logging
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
#args = ["gunicorn", "-b","0.0.0.0:5000","KM(Worker):app" ,"--timeout", "360","--log-level=debug"]
lrw=None
iplist=[]
s = "http://subworker"
sesh=requests.Session()

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

@app.route('/api/worker/start/<string:subworkers>', methods = ['GET'])
def start(subworkers):
    # subworkers = list of ids sep
    global lrw
    global sesh
    global iplist,s
    iplist = [s+str(i)+':4000' for i in subworkers.split("+")]
    if lrw is not None:    #if process is running or has run before
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run
        
        lrw=subprocess.Popen(args)
        for ip in iplist:
            url = ip+'/api/subworker/start'
            #initw = threading.Thread(target=sesh.get, args=(url,))
            #initw.start()                   #start lr(worker) api
            requests.get(url)
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
