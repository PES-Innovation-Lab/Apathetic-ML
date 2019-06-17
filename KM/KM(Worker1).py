
import flask
import requests
import numpy
import numpy as np
from copy import deepcopy

app = flask.Flask(__name__)

user=None

sesh=requests.Session()


class User:
    def __init__(self,dataset):
        self.dataset=dataset
        #recieve dataset from the actual master function
    def init_model(self,combs):
        #receive dataset from the master function
        self.combs = combs
    def find_best_cluster(self):
        final_clusters = []
        for i in range(len(self.combs)):
            final_clusters.append(self.find_cluster(self.combs[i]))
        err = final_clusters[0][1]
        cluster = final_clusters[0][0]
        for i in final_clusters:
            if i[1]<err:
                cluster = i[0]
                err = i[1]
        #send cluster to the master node and append to the self.final_clusters
        return cluster,err
    def find_cluster(self,ini_means):
        old_means = ini_means
        new_means = self.compute_new_mean(deepcopy(old_means))
        if np.array_equal(old_means,new_means):
            new_cluster = self.classify_cluster(new_means)
            return (new_cluster,self.compute_error(new_cluster))
        else:
            return self.find_cluster(new_means)
    def compute_error(self,clusters):
        error = 0
        for i in clusters:
            mean_i = np.ones(np.array(i).shape)*np.mean(i)
            error += np.square(np.array(i)-mean_i).mean()
        return error
    def compute_new_mean(self,means):
        clusters = self.classify_cluster(means)
        new_means = [np.mean(i) for i in clusters]
        return new_means
    def classify_cluster(self,means):
        cluster = [[] for i in means]
        for i in self.dataset:
            min_err = np.square(i-means[0]).mean()
            mean = 0
            for j in range(len(means)):
                if (min_err>np.square(i-means[j]).mean()):
                    min_err = np.square(i-means[j]).mean()
                    mean = j
            cluster[mean].append(i)
        return np.array(cluster)


@app.route('/api/worker/km/userinit', methods = ['POST'])
def userinit():
    global user
    dataset=np.array(flask.request.json['dataset'])
    user=User(dataset)
    return flask.Response(status = 200)
    
@app.route('/api/worker/km/initmodel', methods = ['POST'])
def initmodel():
    global user
    combs=np.array(flask.request.json['combs'])
    user.init_model(combs)
    return flask.Response(status = 200)
    
@app.route('/api/worker/km/findbestcluster', methods=['POST'])
def fitmodel():
    global user
    global sesh
    ret_clusters,err=user.find_best_cluster()
    cluster = []
    for i in ret_clusters:
        temp1 = []
        for j in i:
            temp11 = []
            for k in j:
                temp11.append(k)
            temp1.append(temp11)
        cluster.append(temp1)
                
    url = 'http://127.0.0.1:4000/api/master/km/recievecombs'
    sesh.post(url,json={'clusters':cluster,'err':err})
    return flask.Response(status = 200)
    
    
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
    
