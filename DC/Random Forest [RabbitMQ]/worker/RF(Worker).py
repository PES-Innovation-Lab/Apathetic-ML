import numpy as np
import pickle
import requests
import flask
import json
import pandas as pd
import time
import gc
from flask_cors import CORS
from jsonpickle import encode,decode
import socket
import sys
def imports():
    global DT,Process,Queue,pika,dumps,loads,ast
    from sklearn.tree import DecisionTreeClassifier as DT
    from multiprocessing import Process, Queue
    import pika
    from json import dumps,loads
    import ast
    
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
        #filestart = time.time()
        #q = Queue()
        #proc = []
        # for i in range(len(DTs)):
        #     t_file_name = s+str(i)+'.pkl'
        #     p = Process(target=file_dumper,args=(q,t_file_name,DTs[i]))
        #     p.start()
        #     proc.append(p)
            
        # for i in range(len(DTs)):
        #     proc[i].join()
        #     dts.append(q.get()) ####DANGER
        #     #d_temp_file.close()
        
        #filestop = time.time()
        #v = time.time()
        #with open("out",'a') as standardout:
        #    print("FIT TIME",v-z,file=standardout)
        with open("out",'a') as standardout:
            print("FIT COMPLETE",file=standardout)
        return DTs
                
if __name__ == '__main__':
    imports()
    global channel,connection
    a = socket.gethostname()
    a = a[:a.find('-')]
    a = a[-1]
    rtopic='m2w'+a
    user=None
    #credentials = pika.PlainCredentials('user', 'bitnami')
    parameters = pika.ConnectionParameters('rabbit-svr')
    #consumer = KafkaConsumer(rtopic,bootstrap_servers=['kafka-service:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    consumer_conn = pika.BlockingConnection(parameters)
    consumer_channel = consumer_conn.channel()
    consumer_channel.queue_declare(queue=rtopic)
    for method_frame, properties, body in consumer_channel.consume(rtopic):
        # Acknowledge the message
        consumer_channel.basic_ack(method_frame.delivery_tag)
        with open('out','a') as stout:
            print(" [x] Received %r" % body,file=stout,flush=True)
        x=loads(body.decode('utf-8'))
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
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            dts=user.fit(rtopic[3])
            #producer.send('w2m',{'dts':encode(dts)})
            channel.queue_declare(queue='w2m')
            channel.basic_publish(exchange='', routing_key='w2m', body=dumps({'dts':encode(dts)}))
            with open('out','a') as stout:
                print("After send.",file=stout,flush=True)
            connection.close()
    # Cancel the consumer and return any pending messages
    requeued_messages = consumer_channel.cancel()
    print('Requeued %i messages' % requeued_messages)

    # Close the channel and the connection
    consumer_channel.queue_delete(queue=rtopic)
    consumer_channel.close()
    consumer_conn.close()
        