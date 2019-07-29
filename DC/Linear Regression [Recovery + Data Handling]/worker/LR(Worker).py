import flask
import requests
import numpy
import pandas as pd
from flask_cors import CORS
import socket
from json import dumps,loads
from kafka import KafkaProducer,KafkaConsumer
import ast

this_host = socket.gethostname()
myid = this_host[:this_host.find("-")]
myid = myid[-1]
producer = KafkaProducer(acks=True,value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9093','kafka-service:9092','kafka-service:9091','kafka-service:9090'])
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


if __name__ == '__main__':
    
    
    user=None
    consumer = KafkaConsumer(rtopic,bootstrap_servers=['kafka-service:9093','kafka-service:9092','kafka-service:9090','kafka-service:9091'],group_id=rtopic,auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for msg in consumer:
        #x=ast.literal_eval(msg.value)
        x=msg.value
        with open('out','a') as stout:
            print("MESSAGE",x['fun'],file=stout,flush= True)
        if x['fun']=='userinit':
            user=User(x['learning_rate'])
        elif x['fun']=='initmodel':
            try:
                with open("out",'a') as standardout:
                    print("Initialising model",file=standardout)
                
                # # # # dataset = pd.read_csv('USA_Housing.csv')
                # # # # X = dataset.iloc[:,0:5].values
                # # # # y = dataset.iloc[:,5].values
                # # # # from sklearn.model_selection import train_test_split
                # # # # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)

                # # # # with open("out",'a') as standardout:
                # # # #     print("Xshape",X.shape,file=standardout)

                # # # # # Feature Scaling
                # # # # from sklearn.preprocessing import StandardScaler
                # # # # sc_X = StandardScaler()
                # # # # X = sc_X.fit_transform(X)

                # # # # with open("out",'a') as standardout:
                # # # #     print("Xshape",X.shape,file=standardout)

                dataset = pd.read_csv('dataset.csv')
                X = dataset.iloc[:,0].values.reshape(-1,1)
                y = dataset.iloc[:,1].values
                from sklearn.preprocessing import StandardScaler
                X_scaler = StandardScaler()
                X =  X_scaler.fit_transform(X)
                with open("out",'a') as standardout:
                    print("Xshape",X.shape,file=standardout)
                #from sklearn.model_selection import train_test_split
                #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)
                X_train = X
                y_train = y
                batch_size=x['batch_size']
                user.init_model(X,y,batch_size)

            except Exception as e:
                with open("out",'a') as standardout:
                    print(str(e),file=standardout)

        elif x['fun']=='fitmodel':
            # with open("out",'a') as standardout:
            #     count+=1
            #     print(count,file=standardout)
            (wgrad,bgrad)=user.fit_model(numpy.array(x['weights']),x['biases'],x['step'])
            producer.send('w2m',{'wgrad':wgrad.tolist(),'bgrad':bgrad})
            