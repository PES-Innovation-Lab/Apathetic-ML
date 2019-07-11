import flask
import threading
import requests
import concurrent.futures
import time
from flask_cors import CORS
app = flask.Flask(__name__)
CORS(app)
iplist=[]
s = "http://worker"
thread_local = threading.local()

def imports():
    global keras, Sequential, Dense, mnist, K, Graph, Session,load_model,model_from_json
    import keras
    from keras import Sequential
    from keras.layers import Dense
    from keras.datasets import mnist
    from keras import backend as K
    from tensorflow import Graph , Session
    from keras.models import load_model
    from keras.models import model_from_json
    
    with open("out",'a') as stout:
        print("[INFO] Imports complete",file=stout)
        
def get_session():

    if not hasattr(thread_local, "session"):

        thread_local.session = requests.Session()

    return thread_local.session

def preprocess():
    (X_train,y_train),(X_test,y_test) = mnist.load_data()

    X_train_flat = X_train.reshape((X_train.shape[0],-1))

    X_test_flat  = X_test.reshape((X_test.shape[0],-1))

    y_train_oh = keras.utils.to_categorical(y_train,10)

    with open("out",'a') as stout:
        print("[INFO] Preprocessing Done",file=stout)

class GridSearch:
    
    def __init__(self , model_func = None , feed_list_model = None , n_users = 2):
        
        self.model_func = model_func
        
        self.feed_list_model = feed_list_model
        
        self.n_users = n_users

        self.graph = Graph()

        with self.graph.as_default():

            self.sess = Session()

        K.set_session(self.sess)
        
    def fit(self , X_train , y_train , X_test , y_test , batch_size = None , n_epochs = None , feed_list_fit = None):
        
        self.X_train = X_train
        
        self.y_train = y_train
        
        self.feed_list_fit = feed_list_fit
        
        self.X_test = X_test
        
        self.y_test = y_test
        
        self.models = []

        K.set_session(self.sess)

        with self.graph.as_default():
        
            for i in range(len(self.feed_list_model)):
                
                model = self.model_func(self.feed_list_model[i])

                model_json = model.to_json()

                with open("/dev/core/"+str(i) + ".json", "w") as json_file:

                    json_file.write(model_json)

                model.save_weights("/dev/core/"+str(i) + ".h5")

            
        #self.users = [User() for i in range(self.n_users)]
        
        self.users=[]
        futures=[]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for _ in range(self.n_users):
                futures.append(executor.submit(User,iplist[_]))
        for i in futures:
            self.users.append(i.result())
        
        model_per_user = int(len(self.feed_list_model)/self.n_users)
        
        acc = []
        futures=[]
        
        for i in range(model_per_user):
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for j in range(self.n_users):
                    
                    futures.append(executor.submit(self.users[j].fit, self.X_train , self.y_train , self.X_test , self.y_test , i + j*model_per_user , self.feed_list_model[i + j*model_per_user] , batch_size , n_epochs))
            for i in futures:
                acc.append(i.result())        
                
        acc = sorted(acc , key = lambda x: x[0])
        
        return acc
        
        

class User:
    def __init__(self,ip):
    
        sesh=get_session()

        self.ip=ip

        url = self.ip+'/api/worker/gs/userinit'

        sesh.post(url)
    
    def fit(self , X_train , y_train , X_test , y_test , model , feed , batch_size , n_epochs ):
    
        sesh=get_session()     

        url = self.ip+'/api/worker/gs/userfit' 

        r=sesh.post(url,json={'X_test':X_test.tolist(), 'y_test':y_test.tolist(), 'X_train':X_train.tolist() , 'y_train':y_train.tolist() , 'model':model , 'feed':feed , 'batch_size':batch_size , 'n_epochs':n_epochs})

        return (r.json()['acc'],r.json()['feed'])
        


def model_func(lst):
    
    model = Sequential()

    model.add(Dense(input_dim = 784 , units = 256 , activation = 'sigmoid'))

    model.add(Dense(units = 128 , activation = 'sigmoid'))

    model.add(Dense(units = 10 , activation = 'sigmoid'))

    model.compile(optimizer = lst[0] , loss = lst[1] , metrics = ['accuracy'])
    
    return model
    
    
@app.route('/api/master/gs/start/<string:workers>', methods = ['GET'])
def start(workers):
    global iplist
    global s
    iplist = [s+str(i)+':5000' for i in range(0,int(workers))]
    imports()
    preprocess()
    feed_list_model = [['adam','categorical_crossentropy'],
                       ['adam','binary_crossentropy'],
                       ['rmsprop' ,'categorical_crossentropy' ],
                       ['rmsprop' ,'binary_crossentropy' ]
                      ]

    model = GridSearch(model_func , feed_list_model , n_users = int(workers))
    
    a=time.time()
    
    with open("out",'a') as stout:
        print(model.fit(X_train_flat , y_train_oh , X_test_flat , y_test , batch_size = 100 , n_epochs = 10),file=stout)
        print("EXEC TIME", time.time()-a,file=stout)
    return flask.Response(status = 200)
    

if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000)
