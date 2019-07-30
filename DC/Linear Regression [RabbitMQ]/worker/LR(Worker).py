import requests
import numpy
import pandas as pd
import socket
from json import dumps,loads
import pika
parameters = pika.ConnectionParameters('rabbit-svr')
this_host = socket.gethostname()
myid = this_host[:this_host.find("-")]
myid = myid[-1]
rtopic='m2w'+myid   #topic that worker consumes from
with open("out",'a') as standardout:
    print("LR Launched",rtopic,file=standardout)

class User:
    def __init__(self,learning_rate=0.1):
        self.learning_rate = learning_rate
    def init_model(self,train_dataset,train_y,batch_size):
        self.train_dataset = train_dataset      #recieve data from Master
        self.train_y = train_y
        self.batch_size = batch_size
    def fit_model(self,weights,biases,step):
        self.weights = weights      #recieved data
        self.biases = biases
        y_pred = self.train_dataset[self.batch_size*step : self.batch_size*step + self.batch_size]@self.weights + self.biases
        #y_pred = 1/(1+ np.exp(self.train_dataset[self.batch_size*step : self.batch_size*step + self.batch_size]@self.weights + self.biases))
        mse_loss_grad = (y_pred-self.train_y[self.batch_size*step : self.batch_size*step + self.batch_size].reshape(self.batch_size,1))/self.batch_size
        return (self.train_dataset[self.batch_size*step : self.batch_size*step + self.batch_size].T @ mse_loss_grad)*self.learning_rate,numpy.mean((y_pred-self.train_y[self.batch_size*step : self.batch_size*step + self.batch_size].reshape(self.batch_size,1)))*self.learning_rate       #send back updates

def send(topic,data):
    global producer_channel
    producer_channel.basic_publish(exchange='', routing_key=topic, body=dumps(data))

if __name__ == '__main__':
    user=None
    consumer_conn = pika.BlockingConnection(parameters)
    consumer_channel = consumer_conn.channel()
    consumer_channel.queue_declare(queue=rtopic)

    producer_conn = pika.BlockingConnection(parameters)
    producer_channel = consumer_conn.channel()
    producer_channel.queue_declare(queue='w2m')

    for method_frame, properties, body in consumer_channel.consume(rtopic):
        # Acknowledge the message
        consumer_channel.basic_ack(method_frame.delivery_tag)
        x=loads(body.decode('utf-8'))
        with open('out','a') as stout:
            print("MSG",x['fun'],file=stout,flush=True)
            
        if x['fun']=='userinit':
            user=User(x['learning_rate'])
        elif x['fun']=='initmodel':
            try:
                with open("out",'a') as standardout:
                    print("Initialising model",file=standardout)
                
                dataset = pd.read_csv('dataset.csv')
                X = dataset.iloc[:,0].values.reshape(-1,1)
                y = dataset.iloc[:,1].values
                from sklearn.preprocessing import StandardScaler
                X_scaler = StandardScaler()
                X =  X_scaler.fit_transform(X)
                #from sklearn.model_selection import train_test_split
                #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)

                #with open("out",'a') as standardout:
                #    print("Xshape",X.shape,file=standardout)
                with open("out",'a') as standardout:
                    print("Xshape",X.shape,file=standardout)

                batch_size=x['batch_size']
                user.init_model(X,y,batch_size)

            except Exception as e:
                with open("out",'a') as standardout:
                    print(str(e),file=standardout)

        elif x['fun']=='fitmodel':
            (wgrad,bgrad)=user.fit_model(numpy.array(x['weights']),x['biases'],x['step'])
            data = {'wgrad':wgrad.tolist(),'bgrad':bgrad}
            send('w2m',data)