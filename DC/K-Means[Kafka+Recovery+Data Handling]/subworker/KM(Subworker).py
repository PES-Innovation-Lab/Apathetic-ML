
import numpy as np
import socket
from kafka import KafkaConsumer,KafkaProducer
from json import dumps,loads

this_host = socket.gethostname()
myid = this_host[:this_host.find("-")]
producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092','kafka-service:9091','kafka-service:9090','kafka-service:9093'])
rtopic='w2sw'+myid[-2]+myid[-1]   #topic that worker consumes from

with open("out",'a') as standardout:
    print("KM sw Launched",rtopic,file=standardout)

if __name__ == '__main__':
    consumer = KafkaConsumer(rtopic,bootstrap_servers=['kafka-service:9092','kafka-service:9091','kafka-service:9090','kafka-service:9093'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for msg in consumer:
        x=msg.value
        dataset=np.array(x['dataset'])
        means=np.array(x['means'])
        cluster = [[] for i in means]
        for i in dataset:
            min_err = np.square(i-means[0]).mean()
            mean = 0
            for j in range(len(means)):
                if (min_err>np.square(i-means[j]).mean()):
                    min_err =  np.square(i-means[j]).mean()
                    mean = j
            cluster[mean].append(i)
        for i in range(len(cluster)):
            for j in range(len(cluster[i])):
                cluster[i][j]=cluster[i][j].tolist()
        producer.send('sw2w'+myid[-2],{'cluster':cluster})
        
        #with open("out",'a') as stout:
        #    print("Cluster sent",'sw2w'+myid[-2],file = stout,flush = True)

