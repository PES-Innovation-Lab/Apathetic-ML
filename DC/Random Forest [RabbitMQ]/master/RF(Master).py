import numpy as np
import pandas as pd
import requests
import flask
import threading
import time
from flask_cors import CORS
import concurrent.futures
import sys
import pika
app = flask.Flask(__name__)
CORS(app)
topics=[]
#credentials = pika.PlainCredentials('user', 'bitnami')
parameters = pika.ConnectionParameters('rabbit-svr')
def imports():
    global DT,operator,pickle,confusion_matrix,dumps,loads,ast,Process,Queue,encode,decode
    from sklearn.tree import DecisionTreeClassifier as DT
    import operator
    import pickle
    from sklearn.metrics import confusion_matrix
    from json import dumps,loads
    import ast
    from multiprocessing import Process, Queue
    from jsonpickle import encode,decode

def preprocess():
    global dataset,X_train,X_test,y_test,y_train
    #producer = KafkaProducer(acks='all',retries=sys.maxsize,value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092'])
    # Importing the dataset
    dataset = pd.read_csv('Social_Network_Ads.csv')
    X = dataset.iloc[:, [2, 3]].values
    y = dataset.iloc[:, 4].values

    # Splitting the dataset into the Training set and Test set
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.25, random_state = 0)

    # Feature Scaling
    from sklearn.preprocessing import StandardScaler
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test = sc.transform(X_test)

# def file_reader(q,i):
#     try:
#         loaded_model = pickle.load(open('/dev/core/files/'+i, 'rb'))
#         q.put(loaded_model)
#     except Exception as e:
#         time.sleep(5)
#         loaded_model = pickle.load(open('/dev/core/files/'+i, 'rb'))
#         q.put(loaded_model)

class RF:
    def __init__(self,n_users):
        self.n_users = n_users
    def fit(self,n_trees,X,X_test,y,y_test):      
        global topics
        #global producer
        self.n_trees = n_trees
        self.X = X
        self.y = y
        users=[]
        futures=[]
        a = time.time()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for i in range(self.n_users):
                futures.append(executor.submit(User,self.n_trees//self.n_users,topics[i]))
        #producer.flush()
        
        with open("out",'a') as standardout:
            print("FIRST FOR",time.time()-a,file=standardout)
            
        for i in futures:
            users.append(i.result())
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(users[user_i].fit)
                
        #producer.flush()
        self.DTs = consumer(self.n_users)

        with open("out",'a') as standardout:
            print("SECOND FOR",time.time()-a,file=standardout)

        # dts=res
       
        # self.DTs = []
        # proc = []
        # for i in dts:
        #     p = Process(target=file_reader,args=(q,i))
        #     p.start()
        #     proc.append(p)

        # for i in range(len(proc)):
        #     proc[i].join()
        #     self.DTs.append(q.get())

        b = time.time()
        with open("out",'a') as standardout:
            print("TIME TO EXEC:",b-a,file=standardout)
        y_pred = self.predict(X_test)
        cm = confusion_matrix(y_test, y_pred)
        with open("out",'a') as standardout:
            print("Confusion Matrix",cm,file=standardout)
        #producer.close()

    def predict(self,X):
        res = []
        for x in X:
            res_dict = {}
            for i in self.DTs:
                t_res = i[0].predict([x[i[1]]])[0]
                if t_res in res_dict:
                    res_dict[t_res] += 1
                else:
                    res_dict[t_res] = 1
            res.append(max(res_dict.items(), key=operator.itemgetter(1))[0])
        return res

def send(topic,data):
    global parameters
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=topic)
    channel.basic_publish(exchange='', routing_key=topic, body=dumps(data))
    connection.close()

class User:
    def __init__(self,n_trees,topic):
        #global producer
        self.topic=topic
        #producer.send(self.topic,{'fun':'userinit','n_trees':n_trees})
        send(topic,{'fun':'userinit','n_trees':n_trees})
        
    def fit(self):
        #global producer
        #self.topic=topic
        #producer.send(self.topic,{'fun':'workerfit'})
        send(self.topic,{'fun':'workerfit'})

# def consumer(nw):
#     Kconsumer = KafkaConsumer('w2m',bootstrap_servers=['kafka-service:9092'],group_id=None,auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))  
#     cnt=0
#     dts=[]
#     with open('out','a') as stout:
#         print("Consumer start",file=stout,flush=True)
#     for msg in Kconsumer:
#         x=msg.value
#         with open('out','a') as stout:
#             print(cnt,file=stout,flush=True)
#         x = decode(x['dts'])
#         for item in x:
#             dts.append(item)
#         cnt+=1
#         if (cnt == nw):
#             break
#     Kconsumer.close()
#     return dts

def consumer(nw):
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='w2m')
    count = 0
    dts= []
    for method_frame, properties, body in channel.consume('w2m'):
        # Acknowledge the message
        channel.basic_ack(method_frame.delivery_tag)
        #with open('out','a') as stout:
        #    print(" [x] Received %r" % body,file=stout,flush=True)

        x = loads(body.decode('utf-8'))
        with open('out','a') as stout:
            print(count,file=stout,flush=True)
        x = decode(x['dts'])
        for item in x:
            dts.append(item)
        count+=1
        if count == nw:
            break
    # Cancel the consumer and return any pending messages
    requeued_messages = channel.cancel()
    #print('Requeued %i messages' % requeued_messages)
    # Close the channel and the connection
    channel.queue_delete(queue='w2m')
    channel.close()
    connection.close()
    return dts

@app.route('/api/master/rf/start/<string:workers>', methods = ['GET'])
def start(workers):
    imports()
    preprocess()
    global X_test
    global X_train
    global y_test
    global y_train
    abc = workers.split("+")

    global topics
    topics=['m2w'+str(i) for i in range(int(abc[0]))]
    with open('out','a') as stout:
        print("Working with ", workers," workers",file=stout)
    rf = RF(int(abc[0]))
    rf.fit(int(abc[1]),X_train,X_test,y_train,y_test)
    #initw = threading.Thread(target=rf.fit, args=(240,X_train,X_test,y_train,y_test))
    #initw.start() 
    return flask.Response(status = 200)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
