
import numpy as np
import pandas as pd
import itertools 
import math
import requests
import flask
import threading
import concurrent.futures
import time

app = flask.Flask(__name__)

dataset = pd.read_csv('cars.csv')
dataset = dataset.iloc[:,0:7].values
dataset = np.array([np.array([int(j)for j in i])  for i in dataset if '' or ' ' not in i])
print(type(dataset),type(dataset[0]))
cluster=None

iplist=["http://127.0.0.1:5000","http://127.0.0.1:7000"]

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

from sklearn.preprocessing import StandardScaler
sc_X = StandardScaler()
dataset = sc_X.fit_transform(dataset)


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
        
        
@app.route('/api/master/km/start', methods = ['GET'])
def start():
    global cluster
    a=time.time()
    cluster = KCluster(n_users=2)
    clusters = (cluster.fit_cluster(dataset = dataset,k = 3))
    b=time.time()
    print("TIME TO EXEC:",b-a)
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
    app.run(host='127.0.0.1', port=3000)

