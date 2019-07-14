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

def preprocess():
    dataset = pd.read_csv('cars.csv')
    dataset = dataset.iloc[:,0:7].values
    dataset = np.array([np.array([int(j)for j in i])  for i in dataset if '' or ' ' not in i])
    with open('out','a') as stout:
        print(type(dataset),type(dataset[0]),file=stout)
    from sklearn.preprocessing import StandardScaler
    sc_X = StandardScaler()
    dataset = sc_X.fit_transform(dataset)

cluster=None
s = "http://worker"
iplist=[]
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

class KCluster:
    def __init__(self,n_users=10):
        self.n_users = n_users
    def fit_cluster(self,dataset,k=5,n_iters = 10):
        global iplist
        self.dataset = dataset
        self.k = k
        f = math.factorial
        self.n_iters = min(n_iters,f(len(dataset))/(f(k)*f(len(dataset)-k)))
        self.users = [User(ip=iplist[user],dataset = self.dataset) for user in range(self.n_users)]
        self.combs = np.asarray(list(itertools.combinations(self.dataset,self.k))[:n_iters])
        self.combs_split = np.split(self.combs,self.n_users)
        self.final_clusters = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].init_model,self.combs_split[user_i])
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].find_best_cluster)
    def recieve_combs(self,cluster):
        self.final_clusters.append(cluster)
        if len(self.final_clusters)==2:
            err = self.final_clusters[0][1]
            cluster = self.final_clusters[0][0]
            for i in self.final_clusters:
                if i[1]<err:
                    cluster = i[0]
                    err = i[1]
            print(cluster,err)
    def ret_cen(self):
        return np.array([np.mean(x) for x in self.final_clusters])
    def predict(self,X):
        return np.argmin(np.square(self.ret_cen() - X)) + 1

class User:
    def __init__(self,ip,dataset):
        sesh=get_session()
        self.ip=ip
        url = self.ip+'/api/worker/km/userinit'
        sesh.post(url,json={'dataset':dataset.tolist()})
        #send dataset in worker node
        self.dataset = dataset
    def init_model(self,combs):
        sesh=get_session()
        url = self.ip+'/api/worker/km/initmodel'
        sesh.post(url,json={'combs':combs.tolist()})
        #send combs to worker node
        self.combs = combs
    def find_best_cluster(self):
        sesh=get_session()
        url = self.ip+'/api/worker/km/findbestcluster'
        sesh.post(url)
        #send req to fit model with dataset and combs


@app.route('/api/master/km/start/<string:workers>', methods = ['GET'])
def start(workers):
    global cluster
    global iplist,s
    iplist = [s+str(i)+':5000' for i in range(0,int(workers))]
    a=time.time()
    cluster = KCluster(n_users=int(workers))
    clusters = (cluster.fit_cluster(dataset = dataset,k = 3))
    b=time.time()
    with open('out','a') as stout:
        print("TIME TO EXEC:",b-a,file=stout)
    return flask.Response(status = 200)

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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
