import flask
import numpy as np
import json
from kafka import KafkaConsumer,KafkaProducer
from json import dumps
import ast

#CS
#app = flask.Flask(__name__)
producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['localhost:9092'])
rtopic='w2sw1'
'''
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
            cluster[i][j]=cluster[i][j] .tolist()
    return flask.Response(json.JSONEncoder().encode({'cluster':cluster}),mimetype='application/json',status = 200)
'''
#CE
if __name__ == '__main__':
    #CS
    #app.run(host='127.0.0.1', port=9000)
    global producer
    global rtopic
    consumer = KafkaConsumer(rtopic,bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for msg in consumer:
        x=ast.literal_eval(msg.value)
        dataset=np.array(x['dataset'])
        means=np.array(x['means'])
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
        producer.send('sw2w',{'cluster':cluster})
    #CE

