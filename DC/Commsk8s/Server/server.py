from flask import Flask
import flask
import os
import socket
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
        #Outputs the file's contents if any
        m = "<h3>System Core</h3>"
        if not (os.path.exists("/dev/store/datafile")):
                f = open("/dev/store/datafile",'w')
                f.close()
                m+="<br><p>No data in file</p>"
        else:
                f = open("/dev/store/datafile","r")
                file_contents = f.readlines()
                f.close()
                if (file_contents == []):
                        m+="<br><p>No data in file</p>"
                else:
                        for item in file_contents:
                                m+= "<br><p>"+item+"</p>"
        return m

@app.route("/crash")
def crasher():
        #As the name suggests, python process = dead
        try:
                os.system("kill -9 $(ps | grep 'python server'|head -n 1| awk '{$1=$1};1'| cut -d' ' -f 1)")
        except:
                pass

@app.route("/input/<string:input_str>",methods=["GET"])
def take_input(input_str):
        #Takes the input string and stores it in a file
        if not (os.path.exists("/dev/store/datafile")):
                f = open("/dev/store/datafile",'w')
                f.close()
        f = open("/dev/store/datafile","a")
        f.write("\n"+input_str)
        f.close()
        return flask.Response(status=200)

if __name__ == "__main__":
        app.run("0.0.0.0",port=80)
