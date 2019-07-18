import numpy as np
from sklearn.tree import DecisionTreeClassifier as DT
import pickle
import requests
import flask
import json
import pandas as pd
import time
import gc
from multiprocessing import Process, Queue
from kafka import KafkaConsumer,KafkaProducer
from json import dumps
import ast

from flask_cors import CORS
#CS
'''
app = flask.Flask(__name__)
CORS(app)
user=None

sesh=requests.Session()
'''
producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['localhost:9092'])
rtopic='m2w1'
#CE

def file_dumper(q,t_file_name,item):
    d_temp_file = open('/dev/core/files/'+t_file_name, 'wb')
    gc.disable()
    pickle.dump(item, d_temp_file,-1)
    gc.enable()
    d_temp_file.close()
    q.put(t_file_name)
    
class User:
    def __init__(self,n_trees,X,y):
        self.n_trees = n_trees
        self.X = X
        self.y = y
    def fit(self,user_i):
        datasets =[]
        DTs = []
        s = str(user_i)+'_'
        z = time.time()
        with open("out",'a') as standardout:
            print("[Fitting]",file=standardout)
    
        for i in range(self.n_trees):
            data_indeces = np.random.randint(0,self.X.shape[0],self.X.shape[0])
            y_indeces = np.random.randint(0,self.X.shape[1],np.random.randint(1,self.X.shape[1],1)[0])
            temp_d = DT(criterion='entropy')
            temp_d.fit(self.X[data_indeces,y_indeces].reshape(data_indeces.shape[0],y_indeces.shape[0]),self.y[data_indeces])
            DTs.append((temp_d,y_indeces))
        dts = []
        filestart = time.time()
        q = Queue()
        proc = []
        for i in range(len(DTs)):
            t_file_name = s+str(i)+'.pkl'
            #d_temp_file = open('/dev/core/files/'+t_file_name, 'wb')
            #gc.disable()
            #pickle.dump(DTs[i], d_temp_file,-1)
            #gc.enable()
            p = Process(target=file_dumper,args=(q,t_file_name,DTs[i]))
            p.start()
            proc.append(p)
            
        for i in range(len(DTs)):
            proc[i].join()
            dts.append(q.get()) ####DANGER
            #d_temp_file.close()
            
        filestop = time.time()
        v = time.time()
        with open("out",'a') as standardout:
            print("FIT TIME",v-z,file=standardout)
        with open("out",'a') as standardout:
            print("FILE TIME",filestop-filestart,file=standardout)
        return dts
        
        
#CS
'''
@app.route('/api/worker/rf/userinit', methods = ['POST'])
def userinit():
    global user
    try:
        with open("out",'a') as standardout:
            print("Data Reading",file=standardout)
        n_trees=flask.request.json['n_trees']
        #X=np.array(flask.request.json['X'])
        #y=np.array(flask.request.json['y'])
        # Importing the dataset
        dataset = pd.read_csv('Social_Network_Ads.csv')
        X = dataset.iloc[:, [2, 3]].values
        y = dataset.iloc[:, 4].values
        from sklearn.preprocessing import StandardScaler
        sc = StandardScaler()
        X_train = sc.fit_transform(X)

        user=User(n_trees,X_train,y)
        with open("out",'a') as standardout:
            print("Data Ready",file=standardout)
        return flask.Response(status = 200)
    except Exception as e:
        with open("out",'a') as standardout:
            print(str(e),file=standardout)
    
@app.route('/api/worker/rf/workerfit', methods = ['POST'])
def workerfit():
    global user
    user_i=flask.request.json['user_i']
    dts=user.fit(user_i)
    return flask.Response(json.JSONEncoder().encode({'dts':dts}),mimetype='application/json',status = 200)
'''
#CE

if __name__ == '__main__':
    #CS
    #app.run(host='127.0.0.1', port=5000)
    global producer
    global rtopic
    user=None
    consumer = KafkaConsumer(rtopic,bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for msg in consumer:
        x=ast.literal_eval(msg.value)
        if x['fun']=='userinit':
            try:
                with open("out",'a') as standardout:
                    print("Data Reading",file=standardout)
                n_trees=x['n_trees']
                #X=np.array(flask.request.json['X'])
                #y=np.array(flask.request.json['y'])
                # Importing the dataset
                dataset = pd.read_csv('Social_Network_Ads.csv')
                X = dataset.iloc[:, [2, 3]].values
                y = dataset.iloc[:, 4].values
                from sklearn.preprocessing import StandardScaler
                sc = StandardScaler()
                X_train = sc.fit_transform(X)

                user=User(n_trees,X_train,y)
                with open("out",'a') as standardout:
                    print("Data Ready",file=standardout)
            except Exception as e:
                with open("out",'a') as standardout:
                    print(str(e),file=standardout)
        elif x['fun']=='workerfit':
            dts=user.fit(rtopic[3])
            producer.send('w2m',{'dts':dts})
    #CE
