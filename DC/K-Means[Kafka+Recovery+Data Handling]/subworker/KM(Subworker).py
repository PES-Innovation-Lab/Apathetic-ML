
import numpy as np
import socket
from kafka import KafkaConsumer,KafkaProducer
from json import dumps,loads

this_host = socket.gethostname()
myid = this_host[:this_host.find("-")]
myid = myid[-1]
producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['localhost:9092'])
rtopic='w2sw'+myid   #topic that worker consumes from
with open("out",'a') as standardout:
    print("KM sw Launched",rtopic,file=standardout)

if __name__ == '__main__':
    global producer
    global rtopic
    consumer = KafkaConsumer(rtopic,bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
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
        producer.send('sw2w',{'cluster':cluster})

