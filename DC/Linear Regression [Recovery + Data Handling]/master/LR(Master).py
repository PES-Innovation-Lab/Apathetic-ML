import numpy
import pandas as pd
import requests
import flask
import threading
import concurrent.futures
import time
from flask_cors import CORS

app = flask.Flask(__name__)
CORS(app)

def imports():
    global dumps,loads,KafkaProducer,KafkaConsumer,ast,ProfilerMiddleware,cProfile,sys, TopicPartition
    from json import dumps,loads
    from kafka import KafkaProducer,KafkaConsumer, TopicPartition
    import ast
    from werkzeug.contrib.profiler import ProfilerMiddleware
    import cProfile
    import sys

def preprocess(workers):
    global X_train,X_test,y_train,y_test,producers,KConsumer
    dataset = pd.read_csv('USA_Housing.csv')
    X = dataset.iloc[:,0:5].values
    y = dataset.iloc[:,5].values

    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)
    # Feature Scaling
    from sklearn.preprocessing import StandardScaler
    sc_X = StandardScaler()
    X_train = sc_X.fit_transform(X_train)
    X_test = sc_X.transform(X_test)
    producers=[KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092']) for i in range(workers)]
    KConsumer = KafkaConsumer('w2m',bootstrap_servers=['kafka-service:9092'],group_id="master",auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))

    with open("out",'a') as stout:
        print("[INFO] Preprocessing complete",file=stout)

regressor=None

wb = []
#producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092'])
#topics=['m2w1','m2w2']
topics=[]
#thread_local = threading.local()
active = 0
       
class LinearRegressor:
    def __init__(self,learning_rate = 0.1,n_users = 1):
        self.n_users = n_users
        self.learning_rate = learning_rate
        
    def fit(self,train_dataset,train_y,X_test,y_test,n_iters = 200,batch_size = 200):
        global topics, producers,active,TopicPartition
        self.number_of_weights = train_dataset.shape[1]
        self.weights = numpy.random.rand(self.number_of_weights,1)
        self.biases = numpy.random.rand()
        self.train_dataset = train_dataset
        self.train_y = train_y
        self.batch_size = batch_size
        self.users = [User(learning_rate = self.learning_rate,topic=topics[user],producer=producers[user]) for user in range(self.n_users)]
        for i in range(self.n_users):
            producers[i].flush()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].init_model,self.batch_size//self.n_users,producers[user_i])
                #self.users[user_i].init_model(self.batch_size//self.n_users,producers[user_i])
        a = time.time()
        for i in range(self.n_users):
            producers[i].flush()
        
        second = (train_y.shape[0]//self.batch_size)
        with open("out",'a') as stout:
            print("[INFO]",n_iters,second,file=stout)

        #cons = threading.Thread(target=consumer, args=(self.n_users,))
        #active = 1
        #cons.start()
        for j in range(n_iters):
            for step_i in range((train_y.shape[0]//self.batch_size)):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    temp_weight = self.weights
                    temp_biases = self.biases
                    for user_i in range(self.n_users):
                        executor.submit(self.users[user_i].fit_model,self.weights,self.biases,step_i,producers[user_i])
                        #self.users[user_i].fit_model(temp_weight,temp_biases,step_i,producers[user_i])                
                for i in range(self.n_users):
                    producers[i].flush()
                if (KConsumer.partitions_for_topic('w2m')):
                    ps = [TopicPartition('w2m', p) for p in KConsumer.partitions_for_topic('w2m')]
                    KConsumer.resume(*ps)
                consumer(self.n_users)
                ps = [TopicPartition('w2m', p) for p in KConsumer.partitions_for_topic('w2m')]
                KConsumer.pause(*ps)
        #active = 0
        #cons.join()
        b = time.time()
        with open("out",'a') as standardout:
            print("[RESULT] EXEC TIME:",b-a,'s',file=standardout)
        y_pred = self.predict(X_test)
        test_loss=self.test_loss(X_test,y_test)
        with open("out",'a') as standardout:
            print("[RESULT] Test Loss",test_loss,file=standardout)

    def test_loss(self,test_dataset,test_y):
        result = test_dataset @ self.weights + self.biases
        result_loss = numpy.square(result-test_y).mean()
        return result_loss
    def predict(self,X):
        return X@self.weights + self.biases
    def get_weights(self):
        with open("out",'a') as standardout:
            print("Weights:",self.weights," Biases:",self.biases,file=standardout)
        return self.weights,self.biases
    def update_model(self, weights, biases):
        self.weights -= weights      #recieve the updates in a sperate asynchronous function
        self.biases -= biases
        global wb
        wb.append([[self.weights,weights],[self.biases,biases]])

class User:
    def __init__(self,topic,producer,learning_rate=0.1):
        self.topic=topic
        producer.send(self.topic,{'fun':'userinit','learning_rate':learning_rate})
    def init_model(self,batch_size,producer):
        producer.send(self.topic,{'fun':'initmodel','batch_size':batch_size})
        
    def fit_model(self,weights,biases,step,producer):
        producer.send(self.topic,{'fun':'fitmodel','weights':weights.tolist(),'biases':biases,'step':step})
        

def consumer(nw):
    global regressor,KConsumer #,active
    cnt = 0
    for msg in KConsumer:
        x=msg.value
        regressor.update_model(numpy.array(x['wgrad']),x['bgrad'])
        cnt+=1
        if (cnt == nw):
            break
    


@app.route('/api/master/lr/start/<string:workers>', methods = ['GET'])
def start(workers):
    global regressor
    global X_test
    global y_test
    global topics
    imports()
    preprocess(int(workers))
    with open("out",'a') as standardout:
        print("Starting processing\n",file=standardout)
    
    topics=['m2w'+str(i) for i in range(int(workers))]
    
    regressor = LinearRegressor(learning_rate=0.001,n_users=int(workers))
   
    pr = cProfile.Profile()
    pr.enable()
    regressor.fit(X_train,y_train,X_test,y_test,200)
    pr.disable()

    #with open("out",'a') as sys.stdout:
    #    pr.print_stats()

    return flask.Response(status = 200)

@app.route('/api/gimmeresults')
def results():
    global wb
    with open('a','w') as myfile:
        print(wb,file=myfile)

if __name__ == '__main__':
    #app.config['PROFILE'] = True
    #global ProfilerMiddleware
    #app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[30])
    app.run(host='0.0.0.0', port=5000)

