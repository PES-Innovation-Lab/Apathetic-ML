import flask
import requests
import subprocess
import time
import socket
import threading
from flask_cors import CORS
import os
app = flask.Flask(__name__)
CORS(app)
path_to_run = './'          #directory here
py_name = 'KM(Master).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]

lrm=None
s = 'http://worker'
iplist = []

sesh=requests.Session()
os.system('touch out')
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
    global s
    number_of_workers,number_of_subworkers,number_of_clusters,number_of_iterations = workers.split("+")
    
    iplist = [s+str(i)+':4000' for i in range(0,int(number_of_workers))]
    if lrm is not None:    #if process is running
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run    
        lrm=subprocess.Popen(args)     #start lr(master) api
        time.sleep(2)
        with open("out",'a') as standardout:
            print("Starting Operations",file=standardout)
        
        for ip in iplist:
            url = ip+'/api/worker/begin/'+str(number_of_subworkers)
            #initw = threading.Thread(target=sesh.get, args=(url,))
            #initw.start()                   #start lr(worker) api
            requests.get(url)
            time.sleep(2)
        url='http://localhost:5000/api/master/km/start'+'/'+workers
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

        with open("out",'a') as standardout:
            print("Stopping Operations",file=standardout)
        
        return flask.Response(status=200)   #code:ok
    else:                   #process never run
        return flask.Response(status=403)   #code:forbidden


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)

