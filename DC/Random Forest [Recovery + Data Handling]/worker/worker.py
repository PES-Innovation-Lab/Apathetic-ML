import flask
import subprocess
import socket
import os
import requests
import sys
import time
import threading
from flask_cors import CORS

app = flask.Flask(__name__)
CORS(app)
os.system('touch out')
a = socket.gethostname()
service_name = a[:a.find('-')]
cont = 'controller:4000'
path_to_run = ''          #directory here
py_name = 'RF(Worker).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]
lrw = None

# def state_check(controller,selfhost):
#     ret = 'x'
#     while (ret != '1' and ret != '0' ):
#             try:
#                     r = requests.get("http://"+controller+'/'+'api/check/state')
#                     ret = r.content
#                     ret = ret.decode("utf-8")
#             except:
#                     with open("out",'a') as std:
#                             print("Request for state check to controller has failed",file=std)
#             time.sleep(2)
#     with open("out",'a') as std:
#             print("State check complete. State is "+ret,file=std)
#     if (ret == '1'):
#         with open("out",'a') as std:
#                 print("Requesting data from server for restoration.",file=std)
#         k = requests.get("http://"+controller+"/api/gimmepath")
#         path = k.content;path = path.decode('utf-8')
#         r = requests.get("http://"+"localhost:4000"+'/'+'api/worker/start/'+str(path))

# initw = threading.Thread(target=state_check, args=(cont,service_name+':4000'))
# initw.start()

@app.route("/crash")
def crasher():
        #As the name suggests, python process = dead
        try:
                os.system("kill -9 $(ps | grep 'python'|head -n 1| awk '{$1=$1};1'| cut -d' ' -f 1)")
        except:
                pass

@app.route('/')
def hello():
    a = socket.gethostname()
    a= "<html><style>.split {height: 100%;width: 50%;position: fixed;z-index: 1;top: 0;overflow-x: hidden;padding-top: 100px;} .left {left: 0;} .right {right: 0;}</style><h1>Worker - Running</h1><h2>Host Name: "+str(a)+"</h2><div class=\"split left\">"
    proc = subprocess.Popen(["cat", "out"], stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    for item in out.decode('ascii').split('\n'):
        a += "<p>"+str(item)+"</p>"
    return a+"</div></html>"

@app.route('/api/worker/start/<string:filepath>', methods = ['GET'])
def start(filepath):
    #begins processing, first ask for a file, then copy it to local mem for now
    with open("out",'a') as std:
        print("Worker is starting now",file=std)
    a = socket.gethostname()
    url = 'http://controller:4000/api/gimmedata/' + str(a)
    r = requests.get(url)
    file_to_be_used = r.content
    file_to_be_used = file_to_be_used.decode("utf-8") 
    with open("out",'a') as std:
        print("Allocated: ",file_to_be_used,file=std)
    proc = subprocess.Popen(["cp",'/dev/core/data/'+str(file_to_be_used),'/app/'+filepath],stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    with open("out",'a') as std:
        print("Output:",str(out.decode('ascii')),"Stderr:",str(err),file=std)
    return flask.Response(status=200)

@app.route('/api/worker/begin')
def begin():
    global lrw
    if lrw is not None:    #if process is running or has run before
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run 
        with open("out",'a') as std:
            print("[BEGIN]",file=std)
        lrw=subprocess.Popen(args)
        return flask.Response(status=202)

@app.route('/api/worker/stop', methods = ['GET'])
def stop():
    global lrw
    with open("out",'a') as std:
        print("[COMM] Stop request received",file=std)
        
    if lrw is not None:    #if process is running or has completed
        lrw.terminate()
        lrw=None
        with open("out",'a') as std:
            print("[STATE] Terminated",file=std)
        
        return flask.Response(status=200)   #code:ok
    else:                   #process never run
        with open("out",'a') as std:
            print("[INFO] Process wasn't running",file=std)
        
        return flask.Response(status=403)   #code:forbidden

@app.route('/api/worker/reset')
def reset():
    os.system("echo '' > out")
    stop()
    with open("out",'a') as std:
        print("[RESET] Worker has been reset",file=std)
        
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)

