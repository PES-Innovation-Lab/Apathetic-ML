
import flask
import concurrent.futures

def imports():
    global np,keras,Dense,mnist,tf,K,model_from_json,load_model,Graph,Session,time,acc_score,KafkaProducer,KafkaConsumer,TopicPartition,dumps,loads,cProfile,sys,encode,decode
    import numpy as np
    import keras
    from keras.layers import Dense
    from keras.datasets import mnist
    import tensorflow as tf
    from keras import backend as K
    from keras.models import model_from_json
    from keras.models import load_model
    from tensorflow import Graph,Session
    import time
    from kafka import KafkaProducer,KafkaConsumer,TopicPartition
    from json import dumps,loads
    import cProfile
    import sys
    from sklearn.metrics import accuracy_score as acc_score
    from jsonpickle import encode,decode
    
    tf.reset_default_graph()
    
    
def preprocess(workers):
    global X_train,y_train,X_test,y_test,X_train_flat,X_test_flat,y_train_oh,producers,KConsumer
    (X_train,y_train),(X_test,y_test) = mnist.load_data()
    X_train_flat = X_train.reshape((X_train.shape[0],-1))
    X_test_flat  = X_test.reshape((X_test.shape[0],-1))
    y_train_oh = keras.utils.to_categorical(y_train,10)
    
    producers=[KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092']) for i in range(workers)]
    KConsumer = KafkaConsumer('w2m',bootstrap_servers=['kafka-service:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    KConsumer1 = KafkaConsumer('w2m1',bootstrap_servers=['kafka-service:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))

    with open("out",'a') as stout:
        print("[INFO] Preprocessing complete",file=stout)


app = flask.Flask(__name__)

topics=[]

class Sequential:

    def __init__(self , n_users , steps):
        global topics,producers
        
        self.n_users = n_users
        self.steps = steps
        self.graph = Graph()
        
        with self.graph.as_default():
            self.sess = Session()
        
        K.set_session(self.sess)
        
        with self.graph.as_default():
            self.model = keras.Sequential()
        
        self.users=[]
        futures=[]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for _ in range(n_users):
                futures.append(executor.submit(User,topics[_],producers[_]))
        for i in futures:
            self.users.append(i.result())
        for i in range(self.n_users):
            producers[i].flush()
            
    def add(self,layer):
        K.set_session(self.sess)
        
        with self.graph.as_default():
            self.model.add(layer)
        
    def compile(self , optimizer = None , loss = None , metrics = None):
        K.set_session(self.sess)
        
        with self.graph.as_default():
            self.model.compile(optimizer = optimizer , loss = loss , metrics = metrics)
            '''
            model_json = self.model.to_json()
            with open("model.json", "w") as json_file:
                json_file.write(model_json)
            self.model.save_weights("model.h5")
            '''
            jsonmodel=encode(model)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for _ in range(self.n_users):
                    executor.submit(self.users[_].compile,optimizer,loss,metrics,jsonmodel)
            for i in range(self.n_users):
                producers[i].flush()

    def fit(self , X_train , y_train , X_test , y_test , batch_size = None , epochs = None):
        X_train_split = np.array([np.split(_,self.steps) for _ in np.split(X_train,self.n_users)])
        y_train_split = np.array([np.split(_,self.steps) for _ in np.split(y_train,self.n_users)])
        batch_size_step = batch_size
        
        for epoch_i in range(epochs):
            print('EPOCH : ' + str(epoch_i + 1))
            for step_i in range(self.steps):
                print('\tSTEP : ' + str(step_i + 1))
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for user_i in range(self.n_users):
                        print('\t\tUSER : ' + str(user_i + 1))
                        executor.submit(self.users[user_i].fit, X_train_split[user_i][step_i], y_train_split[user_i][step_i], 1, batch_size_step)
                for i in range(self.n_users):
                    producers[i].flush()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for user_i in range(self.n_users):
                        executor.submit(self.users[user_i].accuracy_score,X_test,y_test)
                for i in range(self.n_users):
                    producers[i].flush()
                if (KConsumer.partitions_for_topic('w2m')):
                    ps = [TopicPartition('w2m', p) for p in KConsumer.partitions_for_topic('w2m')]
                    KConsumer.resume(*ps)
                accuracies=consumer(self.n_users)
                ps = [TopicPartition('w2m', p) for p in KConsumer.partitions_for_topic('w2m')]
                KConsumer.pause(*ps)
                
                print('\tACCURACY : ' + str(max(accuracies)))
                self.best_user_id = np.argmax(np.array(accuracies))
                self.users[self.best_user_id].best_model()
                producers[self.best_user_id].flush()
                if (KConsumer1.partitions_for_topic('w2m1')):
                    ps = [TopicPartition('w2m1', p) for p in KConsumer1.partitions_for_topic('w2m1')]
                    KConsumer1.resume(*ps)
                model=consumer1()
                ps = [TopicPartition('w2m1', p) for p in KConsumer1.partitions_for_topic('w2m1')]
                KConsumer1.pause(*ps)
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for user_i in range(self.n_users):
                        if user_i != self.best_user_id:
                            executor.submit(self.users[user_i].update_model,model)
                for i in range(self.n_users):
                    producers[i].flush()
        
        K.set_session(self.sess)
        
        with self.graph.as_default():
            self.model=decode(model)
            #self.model.load_weights('best_model.h5')
     
    def predict(self, X):
        K.set_session(self.sess)
        
        with self.graph.as_default():
            return self.model.predict(X)

        
class User:
    
    def __init__(self, topic, producer):
        self.topic=topic
        self.producer=producer
        self.producer.send(self.topic,{'fun':'userinit'})
        
    def compile(self , optimizer , loss , metrics,jsonmodel):
        self.producer.send(self.topic,{'fun':'usercompile','optimizer':optimizer,'loss':loss,'metrics':metrics,'model':jsonmodel}) 
        
    def fit(self , X_train , y_train , epochs = 1 , batch_size = None):
        self.producer.send(self.topic,{'fun':'userfit','X_train':X_train.tolist(),'y_train':y_train.tolist(),'epochs':epochs,'batch_size':batch_size})

    def accuracy_score(self , X_test , y_test):
        self.producer.send(self.topic,{'fun':'accuracyscore','X_test':X_test.tolist(),'y_test':y_test.tolist()})

    def best_model(self):
        self.producer.send(self.topic,{'fun':'bestmodel'})
        
    def update_model(self,model):
        self.producer.send(self.topic,{'fun':'updatemodel','model':model})

        
def consumer(nw):
    global KConsumer
    cnt = 0
    accuracies=[]
    for msg in KConsumer:
        x=msg.value
        accuracies.append(x['accuracy'])
        cnt+=1
        if (cnt == nw):
            break
    return accuracies
    
def consumer1():
    global KConsumer1
    cnt = 0
    for msg in KConsumer1:
        x=msg.value
        model=x['model']
        cnt+=1
        if (cnt == 1):
            break
    return model    

@app.route('/api/master/nn/start/<string:workers>', methods = ['GET'])
def start(workers):
    imports()
    preprocess(int(workers))
    
    with open("out",'a') as standardout:
        print("Starting processing\n",file=standardout)
    topics=['m2w'+str(i) for i in range(int(workers))]
    
    model = Sequential(n_users = 5 , steps = 5)
    
    model.add(Dense(input_dim = 784 , units = 256 , activation = 'sigmoid'))
    model.add(Dense(units = 128 , activation = 'sigmoid'))
    model.add(Dense(units = 128 , activation = 'sigmoid'))
    model.add(Dense(units = 128 , activation = 'sigmoid'))
    model.add(Dense(units = 128 , activation = 'sigmoid'))    
    model.add(Dense(units = 10 , activation = 'sigmoid'))
    
    model.compile(optimizer = 'adam' , loss = 'binary_crossentropy' , metrics = ['accuracy'])
    
    a = time.time()
    pr = cProfile.Profile()
    pr.enable()
    
    model.fit(X_train_flat , y_train_oh , X_test_flat , y_test , batch_size = 10 , epochs = 20)
    
    pr.disable()
    #with open("out",'a') as sys.stdout:
    #    pr.print_stats()
    
    y_pred = model.predict(X_test_flat)
    y_pred = [np.argmax(i) for i in y_pred]
    acc = acc_score(y_test , y_pred)
    print(acc)
    print(time.time()-a)
    
    return flask.Response(status = 200)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=3000)
