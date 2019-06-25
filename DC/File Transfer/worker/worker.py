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
py_name = 'LR(Worker).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]
lrw = None

def state_check(controller,selfhost):
    ret = 'x'
    while (ret != '1' and ret != '0' ):
            try:
                    r = requests.get("http://"+controller+'/'+'api/check/state')
                    ret = r.content
                    ret = ret.decode("utf-8")
            except:
                    with open("out",'a') as std:
                            print("Request for state check to controller has failed",file=std)
            time.sleep(1)
    with open("out",'a') as std:
            print("State check complete. State is "+ret,file=std)
    if (ret == '1'):
        with open("out",'a') as std:
                print("Requesting data from server for restoration.",file=std)
        k = requests.get("http://"+controller+"/api/gimmepath")
        path = k.content;path = path.decode('utf-8')
        r = requests.get("http://"+selfhost+'/'+'api/worker/start/'+str(path))

initw = threading.Thread(target=state_check, args=(cont,service_name+':4000'))
initw.start()

@app.route("/crash")
def crasher():
        #As the name suggests, python process = dead
        try:
                os.system("kill -9 $(ps | grep 'python'|head -n 1| awk '{$1=$1};1'| cut -d' ' -f 1)")
        except:
                pass

@app.route('/api/worker/reset')
def reset():
    os.system("echo '' > out")
    with open("out",'a') as std:
        print("Worker has been reset",file=std)
    
@app.route('/')
def hello():
    a = socket.gethostname()
    a= "<html><meta http-equiv=\"refresh\" content=\"5\" ><h1>Worker - Running</h1><h2>Host Name: "+str(a)+"</h2>"
    proc = subprocess.Popen(["tac", "out"], stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    a = a + "<p>"+str(out.decode('ascii'))+"</p></html>"
    return a

@app.route('/api/worker/start/<string:filepath>', methods = ['GET'])
def start(filepath):
    #begins processing, first ask for a file, then copy it to local mem for now
    global lrw
    global output
    if lrw is not None:    #if process is running or has run before
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run    
        
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
        lrw=subprocess.Popen(args)
        return flask.Response(status=202)

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

