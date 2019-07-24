import socket
from kafka import KafkaConsumer
#from keras import *

def imports():
    global np,Dense,tf,K,model_from_json,load_model,Graph,Session,acc_score,KafkaProducer,dumps,loads,encode,decode,producer
    import numpy as np
    from keras.layers import Dense
    import tensorflow as tf
    from keras import backend as K
    from keras.models import model_from_json
    from keras.models import load_model
    from tensorflow import Graph,Session
    from kafka import KafkaProducer
    from json import dumps,loads
    from sklearn.metrics import accuracy_score as acc_score
    from jsonpickle import encode,decode
    
    tf.reset_default_graph()
    
    producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092'])


this_host = socket.gethostname()
myid = this_host[:this_host.find("-")]
myid = myid[-1]

rtopic='m2w'+myid   #topic that worker consumes from

with open("out",'a') as standardout:
    print("NN Launched",rtopic,file=standardout)

class User:
    
    def __init__(self):
        imports()
        
        self.graph = Graph()
        
        with self.graph.as_default():
            self.sess = Session()
        
    def compile(self, optimizer, loss, metrics, model):
        K.set_session(self.sess)

        with self.graph.as_default():
            '''
            json_file = open('model.json', 'r')
            loaded_model_json = json_file.read()
            json_file.close()
            loaded_model = model_from_json(loaded_model_json)
            loaded_model.load_weights("model.h5")
            self.model = loaded_model
            '''
            self.model=decode(model)
            print(optimizer,loss,metrics)
            
            self.model.compile(optimizer = optimizer , loss = loss , metrics = metrics)

    def fit(self, X_train, y_train, epochs = 1, batch_size = None):
        K.set_session(self.sess)
        
        with self.graph.as_default():
            self.model.fit(X_train , y_train , epochs = 1 , batch_size = int(batch_size))
    
    def accuracy_score(self, X_test, y_test):
        K.set_session(self.sess)

        with self.graph.as_default():
            y_pred = self.model.predict(X_test)
            y_pred = [np.argmax(i) for i in y_pred]
            accuracy = acc_score(y_test, y_pred)
            return accuracy
    
    def best_model(self):
        K.set_session(self.sess)
        
        with self.graph.as_default():
            return encode(self.model)
            #self.model.save_weights('best_model.h5')
    
    def update_model(self,model):
        K.set_session(self.sess)
        
        with self.graph.as_default():
            self.model=decode(model)
            #self.model.load_weights('best_model.h5')


if __name__ == '__main__':
    user=None
    consumer = KafkaConsumer(rtopic,bootstrap_servers=['kafka-service:9092'],group_id=rtopic,auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for msg in consumer:
        x=msg.value
        if x['fun']=='userinit':
            user=User()
        elif x['fun']=='usercompile':
            optimizer=x['optimizer']
            loss=x['loss']
            metrics=x['metrics']
            model=x['model']
            user.compile(optimizer,loss,metrics,model)
        elif x['fun']=='userfit':
            X_train=np.array(x['X_train'])
            y_train=np.array(x['y_train'])
            epochs=x['epochs']
            batch_size=x['batch_size']
            user.fit(X_train,y_train,epochs,batch_size)
        elif x['fun']=='accuracyscore':
            X_test=np.array(x['X_test'])
            y_test=np.array(x['y_test'])
            accuracy=user.accuracy_score(X_test,y_test)
            producer.send('w2m',{'accuracy':accuracy})
        elif x['fun']=='bestmodel':
            model=user.best_model()
            producer.send('w2m1',{'model':model})
        elif x['fun']=='bestmodel':
            model=x['model']
            user.update_model(model)
            
