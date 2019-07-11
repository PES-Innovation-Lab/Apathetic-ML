import flask
import subprocess
from flask_cors import CORS
app = flask.Flask(__name__)
CORS(app)
path_to_run = './'          #directory here
py_name = 'GS(Worker).py'   #fileName here
args = ["python3", "{}{}".format(path_to_run, py_name)]

lrw=None

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

