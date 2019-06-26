import numpy as np
from sklearn.tree import DecisionTreeClassifier as DT
import pickle
import requests
import flask
import json
import pandas as pd
from flask_cors import CORS
app = flask.Flask(__name__)
CORS(app)
user=None

sesh=requests.Session()

class User:
    def __init__(self,n_trees,X,y):
        self.n_trees = n_trees
        self.X = X
        self.y = y
    def fit(self,user_i):
        datasets =[]
        DTs = []
        s = str(user_i)+'_'
        with open("out",'a') as standardout:
            print("[Fitting]\n",file=standardout)
    
        for i in range(self.n_trees):
            data_indeces = np.random.randint(0,self.X.shape[0],self.X.shape[0])
            y_indeces = np.random.randint(0,self.X.shape[1],np.random.randint(1,self.X.shape[1],1)[0])
            temp_d = DT(criterion='entropy')
            temp_d.fit(self.X[data_indeces,y_indeces].reshape(data_indeces.shape[0],y_indeces.shape[0]),self.y[data_indeces])
            DTs.append((temp_d,y_indeces))
        dts = []
        for i in range(len(DTs)):
            t_file_name = s+str(i)+'.pkl'
            d_temp_file = open('/dev/core/files/'+t_file_name, 'wb')
            pickle.dump(DTs[i], d_temp_file)
            dts.append(t_file_name)
        return dts
        
        
@app.route('/api/worker/rf/userinit', methods = ['POST'])
def userinit():
    global user
    try:
        with open("out",'a') as standardout:
            print("Data Reading",file=standardout)
        n_trees=flask.request.json['n_trees']
        #X=np.array(flask.request.json['X'])
        #y=np.array(flask.request.json['y'])
        # Importing the dataset
        dataset = pd.read_csv('Social_Network_Ads.csv')
        X = dataset.iloc[:, [2, 3]].values
        y = dataset.iloc[:, 4].values
        from sklearn.preprocessing import StandardScaler
        sc = StandardScaler()
        X_train = sc.fit_transform(X)

        user=User(n_trees,X_train,y)
        with open("out",'a') as standardout:
            print("Data Ready",file=standardout)
        return flask.Response(status = 200)
    except Exception as e:
        with open("out",'a') as standardout:
            print(str(e),file=standardout)
    
@app.route('/api/worker/rf/workerfit', methods = ['POST'])
def workerfit():
    global user
    user_i=flask.request.json['user_i']
    dts=user.fit(user_i)
    return flask.Response(json.JSONEncoder().encode({'dts':dts}),mimetype='application/json',status = 200)
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
