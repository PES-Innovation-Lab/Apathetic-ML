import flask
from flask_cors import CORS

app = flask.Flask(__name__)
CORS(app)

cluster=dataset=producers=KConsumer=topics=None

def imports():
    import numpy as np
    import pandas as pd
    import itertools 
    import math
    import concurrent.futures
    import time
    from kafka import KafkaConsumer,KafkaProducer,TopicPartition
    from json import dumps,loads
    import cProfile
    import sys

def preprocess(workers):
    global cluster,dataset,producers,KConsumer,topics
    dataset = pd.read_csv('cars.csv')
    dataset = dataset.iloc[:,0:7].values
    dataset = np.array([np.array([int(j)for j in i])  for i in dataset if '' or ' ' not in i])
    with open("out",'a') as standardout:
        print(type(dataset),type(dataset[0]),file=standardout)
    cluster=None

    #producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['localhost:9092'])
    producers=[KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092']) for i in range(workers)]
    KConsumer = KafkaConsumer('w2m',bootstrap_servers=['kafka-service:9092'],group_id="master",auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    topics=[]

    from sklearn.preprocessing import StandardScaler
    sc_X = StandardScaler()
    dataset = sc_X.fit_transform(dataset)


class KCluster:
    def __init__(self,n_users=10,n_sw=2):
        self.n_users = n_users
        self.n_sw=n_sw
    def fit_cluster(self,dataset,k=5,n_iters = 60):
        global topics
        #global producer
        global producers
        global KConsumer
        self.dataset = dataset
        self.k = k
        f = math.factorial
        with open("out",'a') as standardout:
            print("[FITTING]",file=standardout)
        self.n_iters = min(n_iters,f(len(dataset))/(f(k)*f(len(dataset)-k)))
        #self.users = [User(topic=topics[user],n_sw=self.n_sw) for user in range(self.n_users)]
        self.users = [User(topic=topics[user],n_sw=self.n_sw,producer=producers[user]) for user in range(self.n_users)]
        #producer.flush()
        for i in range(self.n_users):
            producers[i].flush()
        self.combs = np.asarray(list(itertools.combinations(self.dataset,self.k))[:n_iters])
        self.combs_split = np.split(self.combs,self.n_users)
        self.final_clusters = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].init_model,self.combs_split[user_i])
        #producer.flush()
        for i in range(self.n_users):
            producers[i].flush()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].find_best_cluster)
        #producer.flush()
        for i in range(self.n_users):
            producers[i].flush()
        if (KConsumer.partitions_for_topic('w2m')):
            ps = [TopicPartition('w2m', p) for p in KConsumer.partitions_for_topic('w2m')]
            KConsumer.resume(*ps)
        consumer(self.n_users)
        ps = [TopicPartition('w2m', p) for p in KConsumer.partitions_for_topic('w2m')]
        KConsumer.pause(*ps)
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
    
    def ret_cen(self):
        #final_clusters = np.array(self.final_clusters)
        #with open("out",'a') as standardout:
            #print(self.cluster,file=standardout)
        return (np.array([np.mean(x) for x in self.cluster]),len(self.final_clusters))
        #return self.final_clusters

class User:
    def __init__(self,topic,n_sw,producer):  #def __init__(self,topic,n_sw):
        #global producer
        self.producer=producer
        self.topic=topic
        self.producer.send(self.topic,{'fun':'userinit','n_sw':n_sw})   #producer.send(self.topic,{'fun':'userinit','n_sw':n_sw})
    def init_model(self,combs):
        #global producer
        self.topic=topic
        self.producer.send(self.topic,{'fun':'initmodel','combs':combs.tolist()})   #producer.send(self.topic,{'fun':'initmodel','combs':combs.tolist()})
    def find_best_cluster(self):
        #global producer
        self.topic=topic
        self.producer.send(self.topic,{'fun':'findbestcluster'})    #producer.send(self.topic,{'fun':'findbestcluster'})
        
def consumer(nw):
    global cluster,KConsumer
    cnt=0
    for msg in KConsumer:
        x=msg.value
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
    global topics,dataset
    abc = workers.split("+")
    number_of_workers = int(abc[0])
    imports()
    preprocess(number_of_workers)
    with open("out",'a') as standardout:
        print("Starting processing\n",file=standardout)
    topics=['m2w'+str(i) for i in range(len(number_of_workers))]
    a=time.time()
    cluster = KCluster(n_users=number_of_workers,n_sw=int(abc[1]))
    pr = cProfile.Profile()
    pr.enable()
    cluster.fit_cluster(dataset = dataset,k = 3,n_iters=int(abc[2]))
    pr.disable()

    #with open("out",'a') as sys.stdout:
    #    pr.print_stats()
    b=time.time()
    with open("out",'a') as standardout:
            print("EXEC TIME:",b-a,'s',file=standardout)
            print("Means, Number of workers",cluster.ret_cen(),file=standardout)
    return flask.Response(status = 200)
    
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
