import numpy as np
import pandas as pd
from copy import deepcopy
from kafka import KafkaConsumer,KafkaProducer,TopicPartition
from json import dumps,loads
import socket
import concurrent.futures
topics=[]
this_host = socket.gethostname()
myid = this_host[:this_host.find("-")]
myid = myid[-1]
producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092','kafka-service:9091','kafka-service:9090','kafka-service:9093'])
producers=[]
KConsumer = KafkaConsumer('sw2w'+myid,bootstrap_servers=['kafka-service:9092','kafka-service:9091','kafka-service:9090','kafka-service:9093'],group_id=None,auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
mrtopic='m2w'+myid
with open("out",'a') as standardout:
    print("KM Launched",mrtopic,'sw2w'+myid,file=standardout)

class User:
    def __init__(self,dataset):
        self.dataset=dataset
    def init_model(self,combs):
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
        global producers
        global KConsumer
        dataset_batches=np.split(self.dataset,len(topics))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(len(topics)):
                #executor.submit(classify,dataset_batches[user_i],deepcopy(means),topics[user_i])
                executor.submit(classify,dataset_batches[user_i],deepcopy(means),topics[user_i],producers[user_i])
        #producer.flush()
        for i in range(len(topics)):
            producers[i].flush()
        #if (KConsumer.partitions_for_topic('sw2w'+myid)):
        #    ps = [TopicPartition('sw2w'+myid, p) for p in KConsumer.partitions_for_topic('sw2w'+myid)]
        #    KConsumer.resume(*ps)
        a=consumersw(len(topics),means)
        #ps = [TopicPartition('sw2w'+myid, p) for p in KConsumer.partitions_for_topic('sw2w'+myid)]
        #KConsumer.pause(*ps)
        return a
        
def classify(dataset,means,topic,producer):  #def classify(dataset,means,topic):
    #global producer
    producer.send(topic,{'fun':'classify','dataset':dataset.tolist(),'means':np.array(means).tolist()})
    
def consumersw(nw,means):
    global KConsumer
    cnt=0
    futures=[]
    for msg in KConsumer:
        x=msg.value
        futures.append(x['cluster'])
        cnt+=1
        #with open('out','a') as stout:
        #    print("Received",cnt,file=stout,flush=True)
        if cnt==nw:
            break
    a=futures[0]
    for i in range(len(means)):
        for j in range(1,len(futures)):
            a[i].extend(futures[j][i])
    return np.array(a)

    
if __name__ == '__main__':
    #global producer,mrtopic,topics,myid

    user=None
    consumer = KafkaConsumer(mrtopic,bootstrap_servers=['kafka-service:9092','kafka-service:9091','kafka-service:9090','kafka-service:9093'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for msg in consumer:
        x=msg.value
        with open('out','a') as stout:
            print("Received a message",x['fun'],file=stout,flush=True)
        if x['fun']=='userinit':
            dataset = pd.read_csv('cars.csv')
            dataset = dataset.iloc[:,0:7].values
            dataset = np.array([np.array([int(j)for j in i])  for i in dataset if '' or ' ' not in i])
            from sklearn.preprocessing import StandardScaler
            sc_X = StandardScaler()
            dataset = sc_X.fit_transform(dataset)
            dataset = np.array(dataset.tolist())
            n_sw=x['n_sw']
            topics=['w2sw'+myid+str(i) for i in range(n_sw)]
            producers=[KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092','kafka-service:9091','kafka-service:9090','kafka-service:9093']) for i in range(n_sw)]
            user=User(dataset)
        elif x['fun']=='initmodel':
            combs=np.array(x['combs'])
            user.init_model(combs)
        elif x['fun']=='findbestcluster':
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
