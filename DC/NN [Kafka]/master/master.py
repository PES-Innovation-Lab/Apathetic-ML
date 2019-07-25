import flask
import requests
import subprocess
import time
import threading
from flask_cors import CORS
import os
import socket

app = flask.Flask(__name__)

path_to_run = './'          #directory here
py_name = 'NN(Master).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]
CORS(app)

lrm=None

iplist=[]
s = 'http://worker'
sesh=requests.Session()
os.system('touch out')
os.system("echo '' > out")

@app.route('/')
def hello():
    a = socket.gethostname()
    a= "<html><style>.split {height: 100%;width: 50%;position: fixed;z-index: 1;top: 0;overflow-x: hidden;padding-top: 100px;} .left {left: 0;} .right {right: 0;}</style><h1>Master - Running</h1><h2>Host Name: "+str(a)+"</h2><div class=\"split left\">"
    proc = subprocess.Popen(["cat", "out"], stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    for item in out.decode('ascii').split('\n'):
        a += "<p>"+str(item)+"</p>"
    return a+"</div></html>"


@app.route('/api/master/start/<string:workers>', methods = ['GET'])
def start(workers):
    global lrm
    global sesh
    global iplist
    global s

    iplist = [s+str(i)+':4000' for i in range(0,int(workers))]

    if lrm is not None:    #if process is running
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run
        lrm=subprocess.Popen(args)     #start lr(master) api
        time.sleep(2)
        for ip in iplist:
            url = ip+'/api/worker/begin'
            initw = threading.Thread(target=sesh.get, args=(url,))
            initw.start()                   #start lr(worker) api
            time.sleep(2)
        #time.sleep(3)
        url='http://localhost:5000/api/master/nn/start/' + str(workers)
        #initmodel = threading.Thread(target=sesh.get, args=(url,))
        #initmodel.start()               #begin training
        requests.get(url)
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
        with open("out",'a') as stout:
            print("STOPPED",file=stout)

        return flask.Response(status=200)   #code:ok
    else:                   #process never run
        return flask.Response(status=403)   #code:forbidden


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)
