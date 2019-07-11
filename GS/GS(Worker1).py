import flask
import numpy as np
import json
from sklearn.metrics import accuracy_score as acc_score
from tensorflow import Graph , Session
from keras.models import load_model
from keras.models import model_from_json
from keras import backend as K


app = flask.Flask(__name__)

user=None

class User:
    def __init__(self):
        
        self.graph = Graph()

        with self.graph.as_default():

            self.sess = Session()

        K.set_session(self.sess)
    
    def fit(self , X_train , y_train , X_test , y_test , model , feed , batch_size , n_epochs ):

        K.set_session(self.sess)

        with self.graph.as_default():
        
            json_file = open(str(model) + '.json', 'r')

            loaded_model_json = json_file.read()

            json_file.close()

            loaded_model = model_from_json(loaded_model_json)

            loaded_model.load_weights(str(model) + ".h5")

            model = loaded_model

            model.compile(optimizer = feed[0] , loss = [feed[1]])

            model.fit(X_train , y_train , batch_size , n_epochs)
            
            y_pred = model.predict(X_test)
            
            y_pred = [np.argmax(i) for i in y_pred]

            acc = acc_score(y_test , y_pred)

            return (acc , feed)
        
@app.route('/api/worker/gs/userinit', methods = ['POST'])
def userinit():

    global user

    user=User()

    return flask.Response(status = 200)
    
@app.route('/api/worker/gs/userfit', methods = ['POST'])
def accuracyscore():

    global user

    X_test=np.array(flask.request.json['X_test'])

    y_test=np.array(flask.request.json['y_test'])
    
    X_train=np.array(flask.request.json['X_train'])

    y_train=np.array(flask.request.json['y_train'])
    
    model=flask.request.json['model']
    
    feed=flask.request.json['feed']
    
    batch_size=flask.request.json['batch_size']
    
    n_epochs=flask.request.json['n_epochs']

    (acc , feed)=user.fit(X_train , y_train , X_test , y_test , model , feed , batch_size , n_epochs)

    return flask.Response(json.JSONEncoder().encode({'acc':acc,'feed':feed}),mimetype='application/json',status = 200)
    
if __name__ == '__main__':

    app.run(host='127.0.0.1', port=5000)
