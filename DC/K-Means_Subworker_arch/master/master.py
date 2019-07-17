import flask
import requests
import subprocess
import time
import threading
import os
import socket
import logging
from flask_cors import CORS
app = flask.Flask(__name__)
CORS(app)
if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
path_to_run = './'          #directory here
py_name = 'KM(Master).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]
#args = ["gunicorn", "-b","0.0.0.0:5000","KM(Master):app" ,"--timeout", "360","--log-level=debug"]
lrm=None
s = "http://worker"
iplist=[]
os.system("touch out")
sesh=requests.Session()
os.system("echo '' > out")

@app.route('/')
def hello():
    a = socket.gethostname()
    a= "<html><meta http-equiv=\"refresh\" content=\"5\" ><style>.split {height: 100%;width: 50%;position: fixed;z-index: 1;top: 0;overflow-x: hidden;padding-top: 100px;} .left {left: 0;} .right {right: 0;}</style><h1>Master - Running</h1><h2>Host Name: "+str(a)+"</h2><div class=\"split left\">"
    proc = subprocess.Popen(["tac", "out"], stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    for item in out.decode('ascii').split('\n'):
        a += "<p>"+str(item)+"</p>"
    return a+"</div></html>"
subworkerlist = ["0+3","1+2"]

@app.route('/api/master/start/<string:workers>', methods = ['GET'])
def start(workers):
    global lrm
    global sesh
    global iplist,s
    iplist = [s+str(i)+':4000' for i in range(0,int(workers))]
    print(iplist)
    if lrm is not None:    #if process is running
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run
        lrm=subprocess.Popen(args)     #start lr(master) api
        time.sleep(1)
        for ip in range(len(iplist)):
            url = iplist[ip]+'/api/worker/start/'+subworkerlist[ip]
            initw = threading.Thread(target=sesh.get, args=(url,))
            initw.start()                   #start lr(worker) api
            time.sleep(1)
        url='http://master:5000/api/master/km/start/' + str(workers)
        initmodel = threading.Thread(target=sesh.get, args=(url,))
        initmodel.start()               #begin training
        return flask.Response(status=202)   #code:accepted

@app.route('/api/master/stop', methods = ['GET'])
def stop():
    global lrm
    global sesh
    global iplist
    if lrm is not None:    #process not completed
        for ip in iplist:
            url = ip+'/api/worker/stop'
            stopw = threading.Thread(target=sesh.get, args=(url,))
            stopw.start()
        lrm.terminate()
        lrm=None
        return flask.Response(status=200)   #code:ok
    else:                   #process never run
        return flask.Response(status=403)   #code:forbidden


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)
