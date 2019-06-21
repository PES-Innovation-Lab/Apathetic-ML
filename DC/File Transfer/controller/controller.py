import flask
import requests
import subprocess
import time
import threading
from flask_cors import CORS
import os

app = flask.Flask(__name__)
CORS(app)
distribution_dict = dict()
file_list = dict()
s = 'http://worker'
iplist = []
os.system('touch out')
os.system("echo '' > out")

@app.route("/api/reset")
def reset():
    global distribution_dict
    global file_list
    distribution_dict = dict()
    file_list = dict()
    os.system("echo '' > out")
    with open("out",'w') as std:
        print("Systems Reset\n",file=std)
    return flask.Response(status=200)
    
@app.route('/')
def hello():
    a= "<html><meta http-equiv=\"refresh\" content=\"5\" ><h1>Controller - Running</h1>"
    proc = subprocess.Popen(["tac", "out"], stdout=subprocess.PIPE)
    (out, err) = proc.communicate()
    a = a + "<p>"+str(out.decode('ascii'))+"</p></html>"
    return a

@app.route('/api/startdeploy', methods = ['POST'])
def start():
    global file_list
    global iplist
    global s
    json = flask.request.json
    
    with open("out",'w') as std:
        print("Reading request from client application\n",file=std)
    with open("out",'a') as std:
        print("Request Data",json,file=std)
    if not 'filename' in json and not 'path' in json and not 'splits' in json:
        flask.abort(401)
    else:
        proc = subprocess.Popen(["chmod","+x","master_script.sh"],stdout=subprocess.PIPE)
        (out, err) = proc.communicate()
        with open("out",'a') as std:
            print("Output:",str(out.decode('ascii')),"Stderr:",str(err),file=std)
        proc = subprocess.Popen(["./master_script.sh",json['splits'],'/dev/shadow/'+ json['filename']],stdout=subprocess.PIPE)
        (out, err) = proc.communicate()
        with open("out",'a') as std:
            print("Output:",str(out.decode('ascii')),"Stderr:",str(err),file=std)
        # proc = subprocess.Popen(["cd",'/dev/shadow/data/'],stdout=subprocess.PIPE)
        # (out, err) = proc.communicate()
        # with open("out",'a') as std:
        #     print("Output:",str(out.decode('ascii')),"Stderr:",str(err),file=std)

        # proc = subprocess.Popen(["ls"],stdout=subprocess.PIPE)
        # (out, err) = proc.communicate()
        # with open("out",'a') as std:
        #     print("Output:",str(out.decode('ascii')),"Stderr:",str(err),file=std)
        
        files = os.listdir('/dev/core/data')
        for item in files:
            file_list[item] = 0
        with open("out",'a') as std:
            print(files,file=std)
            
    iplist = [s+str(i)+':4000' for i in range(0,int(json["splits"]))]
    for item in iplist:
        url = item+'/api/worker/start/'+json['path']
        r = requests.get(url)
        
    return flask.Response(status=200)

def get_an_available_file():
    for item in file_list:
        if (file_list[item] == 0):
            file_list[item] = 1
            return item
    return 0

@app.route('/api/gimmedata/<string:hostname>',methods = ['GET'])
def worker_service(hostname):
    #This guy will maintain a massive list of workers and the data given to them and checkpoints too later
    global distribution_dict
    if hostname in distribution_dict:
        with open("out",'a') as std:
            print("Re-allocated: ",file_allocated,"To:",hostname,file=std)
        return distribution_dict[hostname]
    else:
        file_allocated = get_an_available_file()
        distribution_dict[hostname] = file_allocated
        with open("out",'a') as std:
            print("Allocated: ",file_allocated,"To:",hostname,file=std)
        return file_allocated
        
if __name__ == '__main__':
    #app.run(host='127.0.0.1', port=2000)
    app.run(host='0.0.0.0', port = 4000)
