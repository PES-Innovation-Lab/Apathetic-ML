import flask
import requests
import subprocess
import time
import threading
from flask_cors import CORS
import os
import socket

app = flask.Flask(__name__)
CORS(app)
path_to_run = './'          #directory here
py_name = 'RF(Master).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name),">","standardb"]

lrm=None

s = 'http://worker'
iplist = []

sesh=requests.Session()

os.system("touch out")
os.system("mkdir -p /dev/core/files")

@app.route('/api/master/clear')
def fresh():
    os.system("echo '' > out")
    return flask.Response(status= 200)

@app.route('/')
def hello():
    a = socket.gethostname()
    a= "<html><h1>Master - Running</h1><h2>Host Name: "+str(a)+"</h2><div>"
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
    iplist = [s+str(i)+':4000' for i in range(0,int(workers))]
    if lrm is not None:    #if process is running
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run    
        lrm=subprocess.Popen(args)     #start lr(master) api
        time.sleep(3)
        with open("out",'a') as standardout:
            print("Starting Tasks ",file=standardout)
    
        for ip in iplist:
            url = ip+'/api/worker/begin'
            initw = threading.Thread(target=sesh.get, args=(url,))
            initw.start()                   #start lr(worker) api
            time.sleep(1)
        url='http://localhost:5000/api/master/rf/start'+'/'+str(workers)
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
            sesh.get(url)
            #stopw.start()
        lrm.terminate()
        lrm=None
        fresh()
        #try:
        #    requests.get("http://kafka-service:4000/erase")
        #except Exception as e:
        #    with open("out",'a') as standardout:
        #        print("Something happened",e.message,e.args,file=standardout)
            
        with open("out",'a') as standardout:
            print("Stopped the entire operation\n",file=standardout)

        return flask.Response(status=200)   #code:ok
    else:                   #process never run
        return flask.Response(status=403)   #code:forbidden


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)

