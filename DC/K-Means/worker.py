
import flask
import subprocess
from flask_cors import CORS
import os
app = flask.Flask(__name__)
CORS(app)
path_to_run = './'          #directory here
py_name = 'KM(Worker).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]

lrw=None
os.system('touch out')

@app.route('/')
def hello():
    a= "<html><meta http-equiv=\"refresh\" content=\"5\" ><h1>Worker</h1>"
    proc = subprocess.Popen(["tac", "out"], stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    #a = a + "<p>"+str(out.decode('ascii'))+"</p></html>"
    for item in out.decode('ascii').split('\n'):
        a += "<br><p>"+str(item)+"</p>"
    return a+"</html>"

@app.route('/api/worker/start', methods = ['GET'])
def start():
    global lrw
    global output
    if lrw is not None:    #if process is running or has run before
        return flask.Response(status=409)   #code:conflict
    else:                   #process never run    
        lrw=subprocess.Popen(args)
        with open("out",'a') as standardout:
            print("[BEGIN]",file=standardout)
        
        return flask.Response(status=202)   #code:accepted

@app.route('/api/worker/stop', methods = ['GET'])
def stop():
    global lrw
    if lrw is not None:    #if process is running or has completed
        lrw.terminate()
        lrw=None
        with open("out",'a') as standardout:
            print("[TERMINATED]",file=standardout)
        
        return flask.Response(status=200)   #code:ok
    else:                   #process never run
        return flask.Response(status=403)   #code:forbidden


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)

