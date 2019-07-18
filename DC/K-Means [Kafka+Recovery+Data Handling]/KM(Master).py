
import numpy as np
import pandas as pd
import itertools 
import math
import requests
import flask
import threading
import concurrent.futures
import time
from flask_cors import CORS
app = flask.Flask(__name__)
CORS(app)
# import dataset and assign number of clusters
#dataset = np.asarray([[2,1],[2,2],[3,1],[3,2],[7,5],[7,6],[8,5],[8,6]])
# k = 2
dataset = pd.read_csv('cars.csv')
dataset = dataset.iloc[:,0:7].values
dataset = np.array([np.array([int(j)for j in i])  for i in dataset if '' or ' ' not in i])
with open("out",'a') as standardout:
    print(type(dataset),type(dataset[0]),file=standardout)
cluster=None

#CS
#s = 'http://worker'
producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['localhost:9092'])
#topics=['m2w1','m2w2']
topics=[]
'''
iplist=["http://127.0.0.1:5000","http://127.0.0.1:7000"]

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session
'''
#CE

from sklearn.preprocessing import StandardScaler
sc_X = StandardScaler()
dataset = sc_X.fit_transform(dataset)


class KCluster:
    def __init__(self,n_users=10,n_sw=2):
        self.n_users = n_users
        self.n_sw=n_sw
    def fit_cluster(self,dataset,k=5,n_iters = 60):
        #CS        
        global topics
        global producer
        #global iplist
        #CE
        self.dataset = dataset
        self.k = k
        f = math.factorial
        with open("out",'a') as standardout:
            print("[FITTING]",file=standardout)
        self.n_iters = min(n_iters,f(len(dataset))/(f(k)*f(len(dataset)-k)))
        #CS
        self.users = [User(topic=topics[user],n_sw=self.n_sw) for user in range(self.n_users)]
        producer.flush()
        #CE
        self.combs = np.asarray(list(itertools.combinations(self.dataset,self.k))[:n_iters])
        self.combs_split = np.split(self.combs,self.n_users)
        self.final_clusters = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].init_model,self.combs_split[user_i])
        #CS
        producer.flush()
        cons = threading.Thread(target=consumer, args=(self.n_users,))
        cons.start()
        #CE
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].find_best_cluster)
        #CS
        producer.flush() 
        cons.join()
        #CE
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
    
#CS
class User:
    def __init__(self,topic,n_sw):
        global producer
        self.topic=topic
        producer.send(self.topic,{'fun':'userinit','n_sw':n_sw})
        '''
        sesh=get_session()
        self.ip=ip
        url = self.ip+'/api/worker/km/userinit'
        sesh.post(url,json={'dataset':dataset.tolist()})
        #send dataset in worker node
        self.dataset = dataset
        '''
    def init_model(self,combs):
        global producer
        self.topic=topic
        producer.send(self.topic,{'fun':'initmodel','combs':combs.tolist()})
        '''
        sesh=get_session()
        url = self.ip+'/api/worker/km/initmodel'
        sesh.post(url,json={'combs':combs.tolist()})
        #send combs to worker node
        self.combs = combs
        '''
    def find_best_cluster(self):
        global producer
        self.topic=topic
        producer.send(self.topic,{'fun':'findbestcluster'})
        '''
        sesh=get_session()
        url = self.ip+'/api/worker/km/findbestcluster'
        sesh.post(url)
        #send req to fit model with dataset and combs
        '''
        
def consumer(nw):
    global cluster
    consumer = KafkaConsumer('w2m',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    cnt=0
    for msg in consumer:
        x=ast.literal_eval(msg.value)
        if (cluster is not None) and cnt!=nw:
            clusters=x['clusters']
            err=x['err']
            for i in range(len(clusters)):
                clusters[i]=np.array(clusters[i])
            cluster_a=(clusters,err)
            cluster.recieve_combs(cluster_a)
            cnt+=1
        else:
            break
    consumer.close()
#CE
        
        
@app.route('/api/master/km/start/<string:workers>', methods = ['GET'])
def start(workers):
    global cluster
    #CS
    #global number_of_workers
    global topics
    #global s
    #global iplist
    abc = workers.split("+")
    number_of_workers = int(abc[0])
    topics=['m2w'+str(i+1) for i in range(len(number_of_workers))]
    #iplist= [s+str(i)+':5000' for i in range(0,number_of_workers)]
    #CE
    a=time.time()
    cluster = KCluster(n_users=number_of_workers,n_sw=int(abc[1]))
    cluster.fit_cluster(dataset = dataset,k = 3,n_iters=int(abc[2]))
    b=time.time()
    with open("out",'a') as standardout:
            print("EXEC TIME:",b-a,'s',file=standardout)
            print("Means, Number of workers",cluster.ret_cen(),file=standardout)
    return flask.Response(status = 200)
    
#CS
'''
@app.route('/api/master/km/recievecombs', methods = ['POST'])
def recievecombs():
    global cluster
    clusters=flask.request.json['clusters']
    err=flask.request.json['err']
    for i in range(len(clusters)):
        clusters[i]=np.array(clusters[i])
    cluster_a=(clusters,err)
    cluster.recieve_combs(cluster_a)
    return flask.Response(status = 200)
'''
#CE
    

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
