import numpy as np

import requests

import flask

import threading

import concurrent.futures

import time

from sklearn.metrics import accuracy_score as acc_score

from flask_cors import CORS
app = flask.Flask(__name__)
CORS(app)

iplist=[]
s = 'http://worker'

thread_local = threading.local()

def get_session():

    if not hasattr(thread_local, "session"):

        thread_local.session = requests.Session()

    return thread_local.session
def imports():
    global keras, Sequential,Dense,mnist,tf,K,model_from_json,load_model,Graph,Session
    import keras
    from keras import Sequential
    from keras.layers import Dense
    from keras.datasets import mnist
    import tensorflow as tf
    from keras import backend as K
    from keras.models import model_from_json
    from keras.models import load_model
    from tensorflow import Graph , Session
    tf.reset_default_graph()

def preprocess():
    with open("out",'a') as stout:
        print("Beginning preprocessing",file=stout)
    (X_train,y_train),(X_test,y_test) = mnist.load_data()

    with open("out",'a') as stout:
        print("Data Loaded",file=stout)

    X_train_flat = X_train.reshape((X_train.shape[0],-1))

    X_test_flat  = X_test.reshape((X_test.shape[0],-1))

    y_train_oh = keras.utils.to_categorical(y_train,10)
    with open("out",'a') as stout:
        print("Preprocessing complete",file=stout)

    return (X_train_flat,y_train_oh),(X_test_flat,y_test)

class CustSequential:
    
    def __init__(self , n_users , num_steps):
        
        self.n_users = n_users
        
        self.steps = num_steps

        self.graph = Graph()

        with self.graph.as_default():

            self.sess = Session()

        K.set_session(self.sess)

        with self.graph.as_default():
        
            self.model = keras.Sequential()

        #self.users = [User(iplist[_],_) for _ in range(n_users)]
        self.users=[]
        futures=[]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for v in range(n_users):
                futures.append(executor.submit(User,iplist[v],v))
        for i in futures:
            self.users.append(i.result())
            
    def add(self,layer):

        K.set_session(self.sess)

        with self.graph.as_default():
        
            self.model.add(layer)
        
    def compile(self , optimizer = None , loss = None , metrics = None):

        K.set_session(self.sess)

        with self.graph.as_default():
        
            self.model.compile(optimizer = optimizer , loss = loss , metrics = metrics)

            model_json = self.model.to_json()

            with open("/dev/core/model.json", "w") as json_file:

                json_file.write(model_json)

            self.model.save_weights("/dev/core/model.h5")

            #for _ in range(self.n_users):
                
                #self.users[_].compile(optimizer = optimizer , loss = loss , metrics = metrics)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                for _ in range(self.n_users):
                    executor.submit(self.users[_].compile,optimizer,loss,metrics)

    def fit(self , X_train , y_train , X_test , y_test , batch_size = None , epochs = None):
        
        X_train_split = np.array([np.split(_,self.steps) for _ in np.split(X_train,self.n_users)])
        
        y_train_split = np.array([np.split(_,self.steps) for _ in np.split(y_train,self.n_users)])
        
        batch_size_step = batch_size
        
        for epoch_i in range(epochs):
            with open("out",'a') as stout:
                print('EPOCH : ' + str(epoch_i + 1),file=stout)
            
            for step_i in range(self.steps):
                with open("out",'a') as stout:
                    print('\tSTEP : ' + str(step_i + 1),file=stout)

                #for user_i in range(self.n_users):
                    
                    #print('\t\tUSER : ' + str(user_i + 1))
                    
                    #self.users[user_i].fit(X_train_split[user_i][step_i] , y_train_split[user_i][step_i] , 1 , batch_size_step)

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for user_i in range(self.n_users):
                        #with open("out",'a') as stout:
                            #print('\t\tUSER : ' + str(user_i + 1),file=stout)
                        executor.submit(self.users[user_i].fit, X_train_split[user_i][step_i], y_train_split[user_i][step_i], 1, batch_size_step)

                #accuracies = []
                
                #for user_i in range(self.n_users):
                    
                    #accuracies.append(self.users[user_i].accuracy_score(X_test , y_test))
                
                accuracies=[]
                futures=[]
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for user_i in range(self.n_users):
                        futures.append(executor.submit(self.users[user_i].accuracy_score,X_test,y_test))
                for i in futures:
                    accuracies.append(i.result())
                with open("out",'a') as stout:
                    print('\tACCURACY : ' + str(max(accuracies)),file=stout)
                    
                self.best_user_id = np.argmax(np.array(accuracies))
                
                self.users[self.best_user_id].best_model()

                #for user_i in range(self.n_users):
                    
                    #if user_i != self.best_user_id :
                        
                        #self.users[user_i].update_model()
                        
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for user_i in range(self.n_users):
                        if user_i != self.best_user_id:
                            executor.submit(self.users[user_i].update_model)
        
        K.set_session(self.sess)

        with self.graph.as_default():

            self.model.load_weights('/dev/core/best_model.h5')
     
    def predict(self , X):

        K.set_session(self.sess)

        with self.graph.as_default():
                
            return self.model.predict(X)

        
class User:
    
    def __init__(self , ip, user_id = None):
        
        sesh=get_session()

        self.ip=ip

        url = self.ip+'/api/worker/nn/userinit'

        sesh.post(url,json={'user_id':user_id})
        

        
    def compile(self , optimizer , loss , metrics):
    
        sesh=get_session()     

        url = self.ip+'/api/worker/nn/usercompile'   

        sesh.post(url,json={'optimizer':optimizer,'loss':loss,'metrics':metrics})
        
        

    def fit(self , X_train , y_train , epochs = 1 , batch_size = None):
    
        sesh=get_session()     

        url = self.ip+'/api/worker/nn/userfit'

        sesh.post(url,json={'X_train':X_train.tolist(),'y_train':y_train.tolist(),'epochs':epochs,'batch_size':batch_size})
        

    
    def accuracy_score(self , X_test , y_test):
    
        sesh=get_session()     

        url = self.ip+'/api/worker/nn/accuracyscore' 

        r=sesh.post(url,json={'X_test':X_test.tolist(),'y_test':y_test.tolist()})

        return r.json()['accuracy']

    
    def best_model(self):
        
        sesh=get_session()   

        url = self.ip+'/api/worker/nn/bestmodel'  

        sesh.post(url)
        
    
    def update_model(self):
    
        sesh=get_session()     

        url = self.ip+'/api/worker/nn/updatemodel' 

        sesh.post(url)
        

    
@app.route('/api/master/nn/start/<string:workers>', methods = ['GET'])
def start(workers):
    global s
    global iplist

    iplist = [s+str(i)+':5000' for i in range(0,int(workers))]
    imports()
    with open("out","a") as stout:
        print("Imports completed",file=stout)
    (X_train_flat,y_train_oh),(X_test_flat,y_test) = preprocess()
    
    model = CustSequential(n_users = int(workers) , num_steps = 2)

    model.add(Dense(input_dim = 784 , units = 256 , activation = 'sigmoid'))

    model.add(Dense(units = 128 , activation = 'sigmoid'))
    
    model.add(Dense(units = 128 , activation = 'sigmoid'))
    
    model.add(Dense(units = 128 , activation = 'sigmoid'))
        
    model.add(Dense(units = 128 , activation = 'sigmoid'))    

    model.add(Dense(units = 10 , activation = 'sigmoid'))

    model.compile(optimizer = 'adam' , loss = 'binary_crossentropy' , metrics = ['accuracy'])

    a = time.time()

    model.fit(X_train_flat , y_train_oh , X_test_flat , y_test , batch_size = 10 , epochs = 20)


    y_pred = model.predict(X_test_flat)
            
    y_pred = [np.argmax(i) for i in y_pred]

    acc = acc_score(y_test , y_pred)
    with open("out",'a') as stout:
        print(acc,file=stout)
        print(time.time()-a,file=stout)

    return flask.Response(status = 200)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
