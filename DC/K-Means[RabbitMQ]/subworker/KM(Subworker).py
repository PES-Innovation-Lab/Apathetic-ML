import numpy as np
import socket
import pika
from json import dumps,loads
parameters = pika.ConnectionParameters('rabbit-svr')
this_host = socket.gethostname()
myid = this_host[:this_host.find("-")]

rtopic='w2sw'+myid[-2]+myid[-1]   #topic that worker consumes from

with open("out",'a') as standardout:
    print("KM sw Launched",rtopic,file=standardout)
    
def send(channel,topic,data):
    channel.basic_publish(exchange='', routing_key=topic, body=dumps(data))

if __name__ == '__main__':
    consumer_conn = pika.BlockingConnection(parameters)
    consumer_channel = consumer_conn.channel()
    consumer_channel.queue_declare(queue=rtopic)

    producer_conn = pika.BlockingConnection(parameters)
    producer_channel = consumer_conn.channel()
    producer_channel.queue_declare(queue='sw2w'+myid[-2])

    for method_frame, properties, body in consumer_channel.consume(rtopic):
        # Acknowledge the message
        consumer_channel.basic_ack(method_frame.delivery_tag)
        x=loads(body.decode('utf-8'))
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
        send(producer_channel,'sw2w'+myid[-2],{'cluster':cluster})
        