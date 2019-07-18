
import flask
import requests
import numpy as np
import pandas as pd
from copy import deepcopy
from flask_cors import CORS
#CS
'''
app = flask.Flask(__name__)

user=None

iplist=["http://127.0.0.1:9000","http://127.0.0.1:11000"]

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session
'''
#topics=['w2sw1','w2sw2']
topics=[]
mrtopic='m2w1'
producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['localhost:9092'])
#CE


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
        #CS
        global topics
        dataset_batches=np.split(self.dataset,len(topics))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future=executor.submit(consumersw,len(topics),means)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for user_i in range(len(topics)):
                    executor.submit(classify,dataset_batches[user_i],deepcopy(means),topics[user_i])
        a=future.result()
        '''
        a=futures[0].result()
        for i in range(len(means)):
            for j in range(1,len(futures)):
                a[i].extend(futures[j].result()[i])
        return np.array(a)
        '''
        return a
        
def classify(dataset,means,topic):
    global producer
    producer.send(topic,{'fun':'classify','dataset':dataset.tolist(),'means':np.array(means).tolist()})
    '''
    sesh=get_session()
    url = ip+'/api/subworker/km/classify'
    dataset=dataset.tolist()
    #for i in range(len(dataset)):
        #print(type(dataset[i]))
    #means=means.tolist()
    #for i in range(len(means)):
        #means[i]=means[i].tolist()
    r=sesh.post(url,json={'dataset':dataset,'means':means})
    return r.json()['cluster']
    '''
    
def consumersw(nw,means):
    consumer = KafkaConsumer('sw2w',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    cnt=0
    futures=[]
    for msg in consumer:
        x=ast.literal_eval(msg.value)
        if cnt!=nw:
            futures.append(x['cluster'])
            cnt+=1
        else:
            break
    consumer.close()
    a=futures[0].result()
    for i in range(len(means)):
        for j in range(1,len(futures)):
            a[i].extend(futures[j].result()[i])
    return np.array(a)
#CE
    

#CS
'''
@app.route('/api/worker/km/userinit', methods = ['POST'])
def userinit():
    global user
    #dataset=np.array(flask.request.json['dataset'])
    dataset = pd.read_csv('cars.csv')
    dataset = dataset.iloc[:,0:7].values
    dataset = np.array([np.array([int(j)for j in i])  for i in dataset if '' or ' ' not in i])
    from sklearn.preprocessing import StandardScaler
    sc_X = StandardScaler()
    dataset = sc_X.fit_transform(dataset)
    dataset = np.array(dataset.tolist())
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
                
    url = 'http://master:5000/api/master/km/receivecombs'
    sesh.post(url,json={'clusters':cluster,'err':err})
    return flask.Response(status = 200)
'''    
    
if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=5000)
    global producer
    global mrtopic
    global topics
    user=None
    consumer = KafkaConsumer(mrtopic,bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for msg in consumer:
        x=ast.literal_eval(msg.value)
        if x['fun']=='userinit':
            dataset = pd.read_csv('cars.csv')
            dataset = dataset.iloc[:,0:7].values
            dataset = np.array([np.array([int(j)for j in i])  for i in dataset if '' or ' ' not in i])
            from sklearn.preprocessing import StandardScaler
            sc_X = StandardScaler()
            dataset = sc_X.fit_transform(dataset)
            dataset = np.array(dataset.tolist())
            n_sw=x['n_sw']
            topics=['w2sw'+str(i+1) for i in range(n_sw)]
            user=User(dataset)
        elif x['fun']=='initmodel':
            combs=np.array(x['combs'])
            user.init_model(combs)
        elif x['fun']=='fitmodel':
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
            producer.send('w2m',{'clusters':cluster,'err':err})
#CE    
