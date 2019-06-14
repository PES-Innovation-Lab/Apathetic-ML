from flask import Flask
import flask
import os
import socket
import requests
from flask_cors import CORS
app = Flask(__name__)
CORS(app)

@app.route("/send/<string:outbound_data>")
def send(outbound_data):
        #sends the data to the server for storage
        url = "http://server:80/input/"+outbound_data
        try: 
                result = requests.get(url)
                if (result.status_code != 200):
                        flask.abort(400)
                else:
                        return "<h3>Data Sent</h3>"
        except Exception as e:
                return "<h1>Fucking Failed --- "+str(e)+"</h1>"
            
if (__name__ == '__main__'):
    app.run('0.0.0.0',port=80)
