import flask
import subprocess
import socket
import os
import requests
from flask_cors import CORS

app = flask.Flask(__name__)
CORS(app)
os.system('touch out')
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
    with open("out",'a') as std:
        print("Worker is starting now",file=std)
    a = socket.gethostname()
    url = 'http://controller:4000/api/gimmedata/' + str(a)
    r = requests.get(url)
    file_to_be_used = r.content
    file_to_be_used = file_to_be_used.decode("utf-8") 
    with open("out",'a') as std:
        print("Allocated: ",file_to_be_used,file=std)
    proc = subprocess.Popen(["cp",'/dev/core/data/'+str(file_to_be_used),'/dev/pers/'+filepath],stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    with open("out",'a') as std:
        print("Output:",str(out.decode('ascii')),"Stderr:",str(err),file=std)
    return flask.Response(status=200)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)

