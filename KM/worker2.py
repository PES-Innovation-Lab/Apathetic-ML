
import flask
import requests
import subprocess
import time
import threading

app = flask.Flask(__name__)

path_to_run = './'          #directory here
py_name = 'KM(Worker2).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]

lrw=None

iplist=["http://127.0.0.1:12000","http://127.0.0.1:14000"]

sesh=requests.Session()


@app.route('/api/worker/start', methods = ['GET'])
def start():
    global lrw
    global sesh
    global iplist
    if lrw is not None:    #if process is running or has run before
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run    
        lrw=subprocess.Popen(args)
        time.sleep(0.5)
        for ip in iplist:
            url = ip+'/api/subworker/start'
            initw = threading.Thread(target=sesh.get, args=(url,))
            initw.start()                   #start lr(worker) api
            time.sleep(0.5)
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
    app.run(host='127.0.0.1', port=6000)

