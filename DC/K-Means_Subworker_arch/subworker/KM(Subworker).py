import flask
import numpy as np
import json
from flask_cors import CORS
app = flask.Flask(__name__)
CORS(app)
@app.route('/api/subworker/km/classify', methods = ['POST'])
def classify():
    dataset=np.array(flask.request.json['dataset'])
    means=np.array(flask.request.json['means'])
    cluster = [[] for i in means]
    for i in dataset:
        min_err = np.square(i-means[0]).mean()
        mean = 0
        for j in range(len(means)):
            if (min_err>np.square(i-means[j]).mean()):
                min_err =  np.square(i-means[j]).mean()
                mean = j
        cluster[mean].append(i)
    for i in range(len(cluster)):
        for j in range(len(cluster[i])):
            cluster[i][j]=cluster[i][j].tolist()
    return flask.Response(json.JSONEncoder().encode({'cluster':cluster}),mimetype='application/json',status = 200)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
