import flask
import subprocess
from flask_cors import CORS
import socket
import os
import sys
app = flask.Flask(__name__)
CORS(app)
path_to_run = './'          #directory here
py_name = 'GS(Worker).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]

lrw=None
os.system("touch out")
@app.route('/')
def hello():
    a = socket.gethostname()
    a= "<html><meta http-equiv=\"refresh\" content=\"5\" ><style>.split {height: 100%;width: 50%;position: fixed;z-index: 1;top: 0;overflow-x: hidden;padding-top: 100px;} .left {left: 0;} .right {right: 0;}</style><h1>Worker - Running</h1><h2>Host Name: "+str(a)+"</h2><div class=\"split left\">"
    proc = subprocess.Popen(["tac", "out"], stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    lines = out.decode('ascii').split('\n')
    for item in lines[:20]:
        a += "<p>"+str(item)+"</p>"
    return a+"</div></html>"

@app.route('/api/worker/start', methods = ['GET'])
def start():
    global lrw
    global output
    if lrw is not None:    #if process is running or has run before
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run    
        lrw=subprocess.Popen(args)
        return flask.Response(status=202)   #code:accepted

@app.route('/api/worker/stop', methods = ['GET'])
def stop():
    global lrw
    if lrw is not None:    #if process is running or has completed
        lrw.terminate()
        lrw=None
        return flask.Response(status=200)   #code:ok
    else:                   #process never run
        return flask.Response(status=403)   #code:forbidden


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)

