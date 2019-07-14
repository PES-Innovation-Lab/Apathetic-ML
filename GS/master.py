
import flask
import requests
import subprocess
import time
import threading

app = flask.Flask(__name__)

path_to_run = './'          #directory here
py_name = 'GS(Master).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]

lrm=None

iplist=["http://127.0.0.1:4000","http://127.0.0.1:6000"]

sesh=requests.Session()


@app.route('/api/master/start', methods = ['GET'])
def start():
    global lrm
    global sesh
    global iplist
    if lrm is not None:    #if process is running
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run    
        lrm=subprocess.Popen(args)     #start lr(master) api
        time.sleep(3)
        for ip in iplist:
            url = ip+'/api/worker/start'
            initw = threading.Thread(target=sesh.get, args=(url,))
            initw.start()                   #start lr(worker) api
            time.sleep(3)
        time.sleep(3)
        url='http://127.0.0.1:3000/api/master/gs/start'
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
    app.run(host='127.0.0.1', port=2000)

