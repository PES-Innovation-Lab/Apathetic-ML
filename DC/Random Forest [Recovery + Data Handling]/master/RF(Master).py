
import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier as DT
import operator
import pickle
import requests
import flask
import threading
import concurrent.futures
from sklearn.metrics import confusion_matrix
import time
from flask_cors import  CORS
app = flask.Flask(__name__)
CORS(app)
#iplist=["http://127.0.0.1:5000","http://127.0.0.1:7000"]
s = 'http://worker'
iplist = []

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

# Importing the dataset
dataset = pd.read_csv('Social_Network_Ads.csv')
X = dataset.iloc[:, [2, 3]].values
y = dataset.iloc[:, 4].values

# Splitting the dataset into the Training set and Test set
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.25, random_state = 0)

# Feature Scaling
from sklearn.preprocessing import StandardScaler
sc = StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)


class RF:
    def __init__(self,n_users):
        self.n_users = n_users
    def fit(self,n_trees,X,X_test,y,y_test):
        global iplist
        self.n_trees = n_trees
        self.X = X
        self.y = y
        users=[]
        futures=[]
        a = time.time()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for i in range(self.n_users):
                #futures.append(executor.submit(User,self.n_trees//self.n_users,X,y,iplist[i]))
                futures.append(executor.submit(User,self.n_trees//self.n_users,iplist[i]))
        with open("out",'a') as standardout:
            print("FIRST FOR",time.time()-a,file=standardout)
            
        for i in futures:
            users.append(i.result())
        #users = [User(self.n_trees//self.n_users,X,y) for i in range(self.n_users)]
        dts = []
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for i in range(self.n_users):
                futures.append(executor.submit(users[i].fit,i))
        with open("out",'a') as standardout:
            print("SECOND FOR",time.time()-a,file=standardout)
        
        for i in futures:
            dts.extend(i.result())
        #for i in range(self.n_users):
        #    dts.extend(users[i].fit(i))
        self.DTs = []
        for i in dts:
            loaded_model = pickle.load(open('/dev/core/files/'+i, 'rb'))
            self.DTs.append(loaded_model)
        b = time.time()
        with open("out",'a') as standardout:
            print("TIME TO EXEC:",b-a,file=standardout)
        y_pred = self.predict(X_test)
        cm = confusion_matrix(y_test, y_pred)
        with open("out",'a') as standardout:
            print("Confusion Matrix",cm,file=standardout)

    def predict(self,X):
        res = []
        for x in X:
            res_dict = {}
            for i in self.DTs:
                t_res = i[0].predict([x[i[1]]])[0]
                if t_res in res_dict:
                    res_dict[t_res] += 1
                else:
                    res_dict[t_res] = 1
            res.append(max(res_dict.items(), key=operator.itemgetter(1))[0])
        return res


class User:
    #def __init__(self,n_trees,X,y,ip):
    def __init__(self,n_trees,ip):
        '''
        self.n_trees = n_trees
        self.X = X
        self.y = y
        '''
        sesh=get_session()
        self.ip=ip    
        url = self.ip+'/api/worker/rf/userinit'       #send train dataset and labels to worker nodes
        #sesh.post(url,json={'n_trees':n_trees,'X':X.tolist(),'y':y.tolist()})
        sesh.post(url,json={'n_trees':n_trees})

    def fit(self,user_i):
        sesh=get_session()
        url = self.ip+'/api/worker/rf/workerfit'       #send train dataset and labels to worker nodes
        r=sesh.post(url,json={'user_i':user_i})
        return r.json()['dts']
        '''
        datasets =[]
        DTs = []
        s = str(user_i)+'_'
        for i in range(self.n_trees):
            data_indeces = np.random.randint(0,self.X.shape[0],self.X.shape[0])
            y_indeces = np.random.randint(0,X.shape[1],np.random.randint(1,X.shape[1],1)[0])
            temp_d = DT(criterion='entropy')
            temp_d.fit(self.X[data_indeces,y_indeces].reshape(data_indeces.shape[0],y_indeces.shape[0]),self.y[data_indeces])
            DTs.append((temp_d,y_indeces))
        dts = []
        for i in range(len(DTs)):
            t_file_name = s+str(i)+'.pkl'
            d_temp_file = open(t_file_name, 'wb')
            pickle.dump(DTs[i], d_temp_file)
            dts.append(t_file_name)
        return dts
        '''

@app.route('/api/master/rf/start/<string:workers>', methods = ['GET'])
def start(workers):
    global X_test
    global X_train
    global y_test
    global y_train
    global iplist

    iplist = [s+str(i)+':5000' for i in range(0,int(workers))] 
    rf = RF(int(workers))
    
    #rf.fit(100,X_train,y_train)
    initw = threading.Thread(target=rf.fit, args=(240,X_train,X_test,y_train,y_test))
    initw.start() 
    return flask.Response(status = 200)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


