import flask
from flask_cors import CORS
import concurrent.futures
import threading
app = flask.Flask(__name__)
CORS(app)
import pika
cluster=dataset=topics=None
topics =[]
parameters = pika.ConnectionParameters('rabbit-svr')
cons_connection = pika.BlockingConnection(parameters)
cons_channel = cons_connection.channel()
cons_channel.queue_declare('w2m')
def imports():
    global np,pd,itertools,math, time,dumps,loads,cProfile,sys
    import numpy as np
    import pandas as pd
    import itertools 
    import math
    import time
    from json import dumps,loads
    import cProfile
    import sys

def preprocess(workers):
    global cluster,dataset
    dataset = pd.read_csv('cars.csv')
    dataset = dataset.iloc[:,0:7].values
    dataset = np.array([np.array([int(j)for j in i])  for i in dataset if '' or ' ' not in i])
    with open("out",'a') as standardout:
        print(type(dataset),type(dataset[0]),file=standardout)
    cluster=None
    from sklearn.preprocessing import StandardScaler
    sc_X = StandardScaler()
    dataset = sc_X.fit_transform(dataset)


class KCluster:
    def __init__(self,n_users=10,n_sw=2):
        self.n_users = int(n_users)
        self.n_sw=int(n_sw)
    def fit_cluster(self,dataset,k=5,n_iters = 60):
        global topics
        global connections
        self.dataset = dataset
        self.k = k
        f = math.factorial
        with open("out",'a') as standardout:
            print("[FITTING]",file=standardout)
        self.n_iters = min(n_iters,f(len(dataset))/(f(k)*f(len(dataset)-k)))
        
        self.users = [User(topic=topics[user],n_sw=self.n_sw,connection=connections[user]) for user in range(self.n_users)]
        
        self.combs = np.asarray(list(itertools.combinations(self.dataset,self.k))[:n_iters])
        self.combs_split = np.split(self.combs,self.n_users)
        self.final_clusters = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].init_model,self.combs_split[user_i])
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].find_best_cluster)
        consumer(self.n_users)
        
    def recieve_combs(self,cluster):
        self.final_clusters.append(cluster)
        if len(self.final_clusters)==len(topics): 
            err = self.final_clusters[0][1]
            self.cluster = self.final_clusters[0][0]
            for i in self.final_clusters:
                if i[1]<err:
                    self.cluster = i[0]
                    err = i[1]
            
            with open("out",'a') as standardout:
               print("ERROR",err,file=standardout)
               #print("Cluster",self.cluster,file=standardout)
    
    def ret_cen(self):
        return (np.array([np.mean(x) for x in self.cluster]),len(self.final_clusters))

def send(channel,topic,data):
    channel.basic_publish(exchange='', routing_key=topic, body=dumps(data))

class User:
    def __init__(self,topic,n_sw,connection):
        self.connection = connection
        self.topic=topic
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.topic)
        send(self.channel,self.topic,{'fun':'userinit','n_sw':n_sw})

    def init_model(self,combs):
        send(self.channel,self.topic,{'fun':'initmodel','combs':combs.tolist()})

    def find_best_cluster(self):
        send(self.channel,self.topic,{'fun':'findbestcluster'})
        
def consumer(nw):
    global cons_channel
    cnt=0
    for method_frame, properties, body in cons_channel.consume('w2m'):
        cons_channel.basic_ack(method_frame.delivery_tag)
        x=loads(body.decode('utf-8'))
        clusters=x['clusters']
        err=x['err']
        for i in range(len(clusters)):
            clusters[i]=np.array(clusters[i])
        cluster_a=(clusters,err)
        cluster.recieve_combs(cluster_a)
        cnt+=1
        if cnt==nw:
            break
    
@app.route('/api/master/km/start/<string:workers>', methods = ['GET'])
def start(workers):
    global cluster
    global topics,dataset,connections
    number_of_workers,number_of_subworkers,number_of_clusters,number_of_iterations = workers.split("+")
    
    imports()
    preprocess(int(number_of_workers))
    with open("out",'a') as standardout:
        print("Starting processing\n",file=standardout)

    topics=['m2w'+str(i) for i in range(int(number_of_workers))]
    connections= [pika.BlockingConnection(parameters) for i in range(int(number_of_workers))]
    
    cluster = KCluster(n_users=int(number_of_workers),n_sw=int(number_of_subworkers))
    a=time.time()
    cluster.fit_cluster(dataset = dataset,k = int(number_of_clusters),n_iters=int(number_of_iterations))
    b=time.time()

    with open("out",'a') as standardout:
        print("EXEC TIME:",b-a,'s',file=standardout)
        print("Centroids",cluster.ret_cen(),file=standardout)
    return flask.Response(status = 200)
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
