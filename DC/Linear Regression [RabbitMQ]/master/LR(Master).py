import numpy
import pandas as pd
import requests
import flask
import threading
import concurrent.futures
import time
from flask_cors import CORS
import pika
parameters = pika.ConnectionParameters('rabbit-svr')
cons_connection = pika.BlockingConnection(parameters)
cons_channel = cons_connection.channel()

#prod_connection = pika.BlockingConnection(parameters)
app = flask.Flask(__name__)
CORS(app)

def imports():
    global dumps,loads,cProfile,sys
    from json import dumps,loads
    import ast
    import cProfile
    import sys

def preprocess(workers):
    global X_train,X_test,y_train,y_test
    dataset = pd.read_csv('dataset.csv')
    X = dataset.iloc[:,0].values.reshape(-1,1)
    y = dataset.iloc[:,1].values
    from sklearn.preprocessing import StandardScaler
    X_scaler = StandardScaler()
    X =  X_scaler.fit_transform(X)
   
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)
    
    with open("out",'a') as stout:
        print("[INFO] Preprocessing complete",file=stout)

regressor=None
wb = []
topics=[]
channels = []
#thread_local = threading.local()
       
class LinearRegressor:
    def __init__(self,learning_rate = 0.1,n_users = 1):
        self.n_users = n_users
        self.learning_rate = learning_rate
        
    def fit(self,train_dataset,train_y,X_test,y_test,n_iters = 200,batch_size = 10000):
        global topics,channels
        self.number_of_weights = train_dataset.shape[1]
        self.weights = numpy.random.rand(self.number_of_weights,1)
        self.biases = numpy.random.rand()
        self.train_dataset = train_dataset
        self.train_y = train_y
        self.batch_size = batch_size
        self.users = [User(connection=connections[user],learning_rate = self.learning_rate,topic=topics[user]) for user in range(self.n_users)]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for user_i in range(self.n_users):
                executor.submit(self.users[user_i].init_model,self.batch_size//self.n_users)
                #self.users[user_i].init_model(self.batch_size//self.n_users)
        a = time.time()
        for j in range(n_iters):
            for step_i in range((train_y.shape[0]//self.batch_size)):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    temp_weight = self.weights
                    temp_biases = self.biases
                    for user_i in range(self.n_users):
                        executor.submit(self.users[user_i].fit_model,self.weights,self.biases,step_i)
                        
                consumer(self.n_users)
        
        b = time.time()
        with open("out",'a') as standardout:
            print("[RESULT] EXEC TIME:",b-a,'s',file=standardout)
        y_pred = self.predict(X_test)
        test_loss=self.test_loss(X_test,y_test)
        with open("out",'a') as standardout:
            print("[RESULT] Test Loss",test_loss,file=standardout)

    def test_loss(self,test_dataset,test_y):
        result = test_dataset @ self.weights + self.biases
        result_loss = sum([(test_y[i] - result[i])**2 for i in range(test_y.shape[0])])/test_y.shape[0]
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

def send(channel,topic,data):
    channel.basic_publish(exchange='', routing_key=topic, body=dumps(data))

class User:
    def __init__(self,connection,topic,learning_rate=0.1):
        self.connection = connection
        self.channel = self.connection.channel()
        self.topic= topic
        self.channel.queue_declare(queue=topic)
        send(self.channel,self.topic,{'fun':'userinit','learning_rate':learning_rate})

    def init_model(self,batch_size):
        send(self.channel,self.topic,{'fun':'initmodel','batch_size':batch_size})
        
    def fit_model(self,weights,biases,step):
        send(self.channel,self.topic,{'fun':'fitmodel','weights':weights.tolist(),'biases':biases,'step':step})
    
def consumer(nw):
    cnt = 0
    global cons_channel,regressor
    cons_channel.queue_declare(queue='w2m')
    for method_frame, properties, body in cons_channel.consume('w2m'):
        cons_channel.basic_ack(method_frame.delivery_tag)
        x=loads(body.decode('utf-8'))
        regressor.update_model(numpy.array(x['wgrad']),x['bgrad'])
        cnt+=1
        if (cnt == nw):
            break

@app.route('/api/master/lr/start/<string:specstring>', methods = ['GET'])
def start(specstring):
    global regressor
    global X_test
    global y_test
    global topics,connections
    imports()
    number_of_workers, number_of_iterations,batch_size = specstring.split("+")
    preprocess(int(number_of_workers))
    with open("out",'a') as standardout:
        print("Starting processing with ",number_of_workers,"workers","batch_size",batch_size,"iters",number_of_iterations,file=standardout)
    
    topics=['m2w'+str(i) for i in range(int(number_of_workers))]
    #channels = [prod_connection.channel() for i in range(int(number_of_workers))]
    #channels = prod_connection.channel()
    connections = [pika.BlockingConnection(parameters) for i in range(int(number_of_workers))]
    regressor = LinearRegressor(learning_rate=0.001,n_users=int(number_of_workers))
   
    pr = cProfile.Profile()
    pr.enable()
    regressor.fit(X_train,y_train,X_test,y_test,n_iters=int(number_of_iterations),batch_size=int(batch_size))
    pr.disable()

    with open("out",'a') as sys.stdout:
        pr.print_stats()

    return flask.Response(status = 200)

@app.route('/api/gimmeresults')
def results():
    global wb
    with open('a','w') as myfile:
        print(wb,file=myfile)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

