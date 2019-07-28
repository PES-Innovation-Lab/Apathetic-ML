import flask
import os
import requests
import sys
import time
import subprocess
from flask_cors import CORS

app = flask.Flask(__name__)
CORS(app)
args = ["./start-kafka.sh"]
@app.route("/erase")
def crasher():
        #As the name suggests, python process = dead
        try:
                os.system("rm -rf /tmp/kafka-logs/*")
        except:
                pass
            
        return flask.Response(status=200)

if __name__ == '__main__':
    kafka = subprocess.Popen(args)
    app.run(host='0.0.0.0', port=4000)

