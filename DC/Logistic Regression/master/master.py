
import flask
import requests
import subprocess
import time
import threading
from flask_cors import CORS
import os

app = flask.Flask(__name__)
CORS(app)

#path_to_run = './'          #directory here
path_to_run=''
py_name = 'LR(Master).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]
#args = ["gunicorn", "-b","0.0.0.0:5000", "LR(Master):app","--timeout","120"]
lrm=None

s='http://worker'
iplist=[s + str(i)+':4000' for i in range(0,2)]

sesh=requests.Session()
os.system("touch out")

@app.route('/')
def hello():
    a= "<html><meta http-equiv=\"refresh\" content=\"5\" ><h1>Master</h1>"
    proc = subprocess.Popen(["tac", "out"], stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    a = a + "<p>"+str(out.decode('ascii'))+"</p></html>"
    return a

@app.route('/api/master/start', methods = ['GET'])
def start():
    global lrm
    global sesh
    global iplist
    if lrm is not None:    #if process is running
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run    
        lrm=subprocess.Popen(args)     #start lr(master) api
        time.sleep(0.5)
        for ip in iplist:
            url = ip+'/api/worker/start'
            initw = threading.Thread(target=sesh.get, args=(url,))
            initw.start()                   #start lr(worker) api
            time.sleep(0.5)
        url='http://localhost:5000/api/master/lr/start'

        initmodel = threading.Thread(target=sesh.get, args=(url,))
        initmodel.start()               #begin training
        return flask.Response(status=202)   #code:accepted

@app.route('/api/master/stop', methods = ['GET'])
def stop():
    global lrm
    global sesh
    global iplist
    with open("out",'a') as standardout:
        print("Stopping the entire operation\n",file=standardout)
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
    #app.run(host='127.0.0.1', port=2000)
    app.run(host='0.0.0.0', port = 4000)
