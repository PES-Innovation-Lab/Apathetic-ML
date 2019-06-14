
import flask
import requests
import numpy

app = flask.Flask(__name__)

user=None

sesh=requests.Session()


class User:
    def __init__(self,learning_rate=0.1):
        self.learning_rate = learning_rate
    def init_model(self,train_dataset,train_y,batch_size):
        self.train_dataset = train_dataset      #recieve data from Master
        self.train_y = train_y
        self.batch_size = batch_size
    def fit_model(self,weights,biases,step): 
        self.weights = weights      #recieved data
        self.biases = biases
        y_pred = self.train_dataset[self.batch_size*step : self.batch_size*step + self.batch_size]@self.weights + self.biases
        mse_loss_grad = (y_pred-self.train_y[self.batch_size*step : self.batch_size*step + self.batch_size].reshape(self.batch_size,1))/self.batch_size
        return (self.train_dataset[self.batch_size*step : self.batch_size*step + self.batch_size].T @ mse_loss_grad)*self.learning_rate,numpy.mean((y_pred-self.train_y[self.batch_size*step : self.batch_size*step + self.batch_size].reshape(self.batch_size,1)))*self.learning_rate       #send back updates


@app.route('/api/worker/lr/userinit', methods = ['POST'])
def userinit():
    global user
    lr=flask.request.json['learning_rate']
    user=User(lr)
    return flask.Response(status = 200)
    
@app.route('/api/worker/lr/initmodel', methods = ['POST'])
def initmodel():
    global user
    train_dataset=numpy.array(flask.request.json['train_dataset'])
    train_y=numpy.array(flask.request.json['train_y'])
    batch_size=flask.request.json['batch_size']
    user.init_model(train_dataset,train_y,batch_size)
    return flask.Response(status = 200)
    
@app.route('/api/worker/lr/fitmodel', methods=['POST'])
def fitmodel():
    global user
    global sesh
    weights=numpy.array(flask.request.json['weights'])
    biases=flask.request.json['biases']
    step=flask.request.json['step']
    (weights,biases)=user.fit_model(weights,biases,step)
    url = 'http://127.0.0.1:4000/api/master/lr/updatemodel'
    sesh.post(url,json={'weights':weights.tolist(),'biases':biases})
    return flask.Response(status = 200)
    
    
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)

