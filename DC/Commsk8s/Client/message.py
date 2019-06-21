from flask import Flask
import random
import string
import requests
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
@app.route('/')
def final_mes(stringLength=10):
    letters = string.ascii_lowercase
    mes =  ''.join(random.choice(letters) for i in range(stringLength))
    url = "http://server:80/input/"+mes
    try: 
        result = requests.get(url)
        if (result.status_code != 200):
                flask.abort(400)
        else:
                return "<h3>Data Sent</h3>"
    except Exception as e:
        return "<h1>Fucking Failed --- "+str(e)+"</h1>"
   
if __name__ == '__main__':
    app.run('0.0.0.0',port=80)


