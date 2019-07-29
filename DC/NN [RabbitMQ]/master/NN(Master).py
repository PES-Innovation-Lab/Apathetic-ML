import flask
import concurrent.futures
import pika
parameters = pika.ConnectionParameters('rabbit-svr')

# def decompressBytesToString(inputBytes):
#   """
#   decompress the given byte array (which must be valid
#   compressed gzip data) and return the decoded text (utf-8).
#   """
#   bio = BytesIO()
#   stream = BytesIO(inputBytes)
#   decompressor = gzip.GzipFile(fileobj=stream, mode='r')
#   while True:  # until EOF
#     chunk = decompressor.read(8192)
#     if not chunk:
#       decompressor.close()
#       bio.seek(0)
#       return bio.read().decode("utf-8")
#     bio.write(chunk)
#   return None

# def compressStringToBytes(inputString):
#   """
#   read the given string, encode it in utf-8,
#   compress the data and return it as a byte array.
#   """
#   bio = BytesIO()
#   bio.write(inputString.encode("utf-8"))
#   bio.seek(0)
#   stream = BytesIO()
#   compressor = gzip.GzipFile(fileobj=stream, mode='w')
#   while True:  # until EOF
#     chunk = bio.read(8192)
#     if not chunk:  # EOF?
#       compressor.close()
#       return stream.getvalue()
#     compressor.write(chunk)
    
def imports():
    global parameters,cons_connection,cons_connection1,cons_channel,cons_channel1
    global np,keras,Dense,mnist,tf,K,model_from_json,load_model,Graph,Session,time,acc_score,dumps,loads,cProfile,sys,encode,decode
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
    from json import dumps,loads
    import cProfile
    import sys
    from sklearn.metrics import accuracy_score as acc_score
    from jsonpickle import encode,decode
    cons_connection = pika.BlockingConnection(parameters)
    cons_connection1 = pika.BlockingConnection(parameters)
    cons_channel = cons_connection.channel()
    cons_channel1 = cons_connection1.channel()
    cons_channel.queue_declare(queue='w2m')
    cons_channel1.queue_declare(queue='w2m1')
    tf.reset_default_graph()

def preprocess(workers):
    #global X_train,y_train,X_test,y_test,X_train_flat,y_train_oh
    global X_test_flat
    (X_train,y_train),(X_test,y_test) = mnist.load_data()
    #X_train_flat = X_train.reshape((X_train.shape[0],-1))
    X_test_flat  = X_test.reshape((X_test.shape[0],-1))
    #y_train_oh = keras.utils.to_categorical(y_train,10)
    with open("out",'a') as stout:
        print("[INFO] Preprocessing complete",file=stout)

app = flask.Flask(__name__)
topics=[]
connections = []
class Sequential:
    def __init__(self , n_users , steps):
        global topics,connections
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
                futures.append(executor.submit(User,topics[_],connections[_],self.n_users,self.steps))
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
            '''
            model_json = self.model.to_json()
            with open("model.json", "w") as json_file:
                json_file.write(model_json)
            self.model.save_weights("model.h5")
            '''
            jsonmodel=encode(self.model)
            
            #jsonmodel = compressStringToBytes(jsonmodel)
            #with open('out','a') as stout:
            #    print(jsonmodel,file=stout)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for _ in range(self.n_users):
                    executor.submit(self.users[_].compile,optimizer,loss,metrics,jsonmodel)

    def fit(self ,batch_size = None , epochs = None):    #def fit(self , X_train , y_train , X_test , y_test , batch_size = None , epochs = None):
        #X_train_split = np.array([np.split(_,self.steps) for _ in np.split(X_train_flat,self.n_users)])
        #y_train_split = np.array([np.split(_,self.steps) for _ in np.split(y_train_oh,self.n_users)])
        batch_size_step = batch_size

        for epoch_i in range(epochs):
            with open('out','a') as stout:
                print('EPOCH : ' + str(epoch_i + 1),file=stout)
            for step_i in range(self.steps):
                with open('out','a') as stout:
                    print('STEP : ' + str(step_i + 1),file=stout)

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for user_i in range(self.n_users):
                        with open('out','a') as stout:
                            print('USER : ' + str(user_i + 1),file=stout)
                        executor.submit(self.users[user_i].fit, user_i, step_i, 1, batch_size_step) #executor.submit(self.users[user_i].fit, X_train_split[user_i][step_i], y_train_split[user_i][step_i], 1, batch_size_step)
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for user_i in range(self.n_users):
                        executor.submit(self.users[user_i].accuracy_score)  #executor.submit(self.users[user_i].accuracy_score,X_test,y_test)
                
                accuracies=consumer(self.n_users)
                with open('out','a') as stout:
                    print('ACCURACY : ' + str(max(accuracies)),file=stout)

                self.best_user_id = np.argmax(np.array(accuracies))
                self.users[self.best_user_id].best_model()
                model=consumer1()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for user_i in range(self.n_users):
                        if user_i != self.best_user_id:
                            executor.submit(self.users[user_i].update_model,model)

        K.set_session(self.sess)

        with self.graph.as_default():
            #model = decompressBytesToString(model.encode('utf-8'))
            self.model=decode(model)
            #self.model.load_weights('best_model.h5')

    def predict(self, X):
        K.set_session(self.sess)
        with self.graph.as_default():
            return self.model.predict(X)

class User:
    def __init__(self, topic, connection,n_users,steps):
        self.topic=topic
        self.connection = connection
        self.channel = connection.channel()
        self.channel.queue_declare(queue=topic)
        data = {'fun':'userinit','n_users':n_users,'step':steps}
        self.channel.basic_publish(exchange='', routing_key=self.topic, body=dumps(data))

    def compile(self , optimizer , loss , metrics,jsonmodel):
        data = {'fun':'usercompile','optimizer':optimizer,'loss':loss,'metrics':metrics,'model':jsonmodel}
        self.channel.basic_publish(exchange='', routing_key=self.topic, body=dumps(data))

    def fit(self , user_i , step_i , epochs = 1 , batch_size = None): #def fit(self , X_train , y_train , epochs = 1 , batch_size = None):
        data = {'fun':'userfit','user_i':user_i,'step_i':step_i,'epochs':epochs,'batch_size':batch_size}
        self.channel.basic_publish(exchange='', routing_key=self.topic, body=dumps(data))

    def accuracy_score(self): #def accuracy_score(self , X_test , y_test):
        data = {'fun':'accuracyscore'}
        self.channel.basic_publish(exchange='', routing_key=self.topic, body=dumps(data))

    def best_model(self):
        data = {'fun':'bestmodel'}
        self.channel.basic_publish(exchange='', routing_key=self.topic, body=dumps(data))

    def update_model(self,model):
        data = {'fun':'updatemodel','model':model}
        self.channel.basic_publish(exchange='', routing_key=self.topic, body=dumps(data))

def consumer(nw):
    cnt = 0
    global cons_channel
    cons_channel.queue_declare(queue='w2m')
    accuracies=[]
    for method_frame, properties, body in cons_channel.consume('w2m'):
        cons_channel.basic_ack(method_frame.delivery_tag)
        x=loads(body.decode('utf-8'))
        accuracies.append(x['accuracy'])
        cnt+=1
        if (cnt == nw):
            break
    cons_channel.cancel()
    return accuracies

def consumer1():
    cnt = 0
    global cons_channel1
    cons_channel1.queue_declare(queue='w2m1')
    for method_frame, properties, body in cons_channel1.consume('w2m1'):
        cons_channel1.basic_ack(method_frame.delivery_tag)
        x=loads(body.decode('utf-8'))
        model = x['model']
        cnt+=1
        if (cnt == 1):
            break
    cons_channel1.cancel()
    return model

@app.route('/api/master/nn/start/<string:workers>', methods = ['GET'])
def start(workers):
    imports()
    preprocess(int(workers))
    global topics,parameters,connections

    with open("out",'a') as standardout:
        print("Starting processing\n",file=standardout)
    topics=['m2w'+str(i) for i in range(int(workers))]
    connections = [pika.BlockingConnection(parameters) for i in range(int(workers))]
    
    with open("out",'a') as standardout:
        print("Topics",topics,file=standardout)

    model = Sequential(n_users = int(workers) , steps = 1)

    model.add(Dense(input_dim = 784 , units = 256 , activation = 'sigmoid'))
    model.add(Dense(units = 128 , activation = 'sigmoid'))
    model.add(Dense(units = 128 , activation = 'sigmoid'))
    model.add(Dense(units = 128 , activation = 'sigmoid'))
    model.add(Dense(units = 128 , activation = 'sigmoid'))
    model.add(Dense(units = 10 , activation = 'sigmoid'))

    model.compile(optimizer = 'adam' , loss = 'binary_crossentropy' , metrics = ['accuracy'])

    a = time.time()
    #pr = cProfile.Profile()
    #pr.enable()

    model.fit(batch_size = 10 , epochs = 10)

    #pr.disable()

    y_pred = model.predict(X_test_flat)
    y_pred = [np.argmax(i) for i in y_pred]
    acc = acc_score(y_test , y_pred)
    with open("out",'a') as stout:
        print("Accuracy",acc,file=stout)
        print("TIME",time.time()-a,file=stout)

    cons_channel.queue_delete(queue='w2m')
    cons_channel1.queue_delete(queue='w2m1')
    cons_channel.close()
    cons_channel1.close()
    cons_connection.close()
    cons_connection1.close()

    
    for usr in model.users:
        usr.channel.queue_delete(queue=usr.topic)
    for conn in connections:
        conn.close()
    #with open("out",'a') as sys.stdout:
    #    pr.print_stats()
    
    return flask.Response(status = 200)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

