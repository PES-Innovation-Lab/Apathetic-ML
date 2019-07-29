import socket
#from keras import *
from json import dumps,loads
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
    global np,Dense,tf,K,model_from_json,load_model,Graph,Session,acc_score,encode,decode
    import numpy as np
    from keras.layers import Dense
    import tensorflow as tf
    from keras import backend as K
    from keras.models import model_from_json
    from keras.models import load_model
    from tensorflow import Graph,Session
    from sklearn.metrics import accuracy_score as acc_score
    from jsonpickle import encode,decode

    tf.reset_default_graph()
    
def preprocess(n_users,steps):
    global X_train,y_train,X_test,y_test,X_train_flat,X_test_flat,y_train_oh
    (X_train,y_train),(X_test,y_test) = mnist.load_data()
    X_train_flat = X_train.reshape((X_train.shape[0],-1))
    X_test_flat  = X_test.reshape((X_test.shape[0],-1))
    y_train_oh = keras.utils.to_categorical(y_train,10)
    
    X_train_split = np.array([np.split(_,steps) for _ in np.split(X_train_flat,n_users)])
    y_train_split = np.array([np.split(_,steps) for _ in np.split(y_train_oh,n_users)])


this_host = socket.gethostname()
myid = this_host[:this_host.find("-")]
myid = myid[-1]

rtopic='m2w'+myid   #topic that worker consumes from

with open("out",'a') as standardout:
    print("NN Launched",rtopic,file=standardout)

class User:

    def __init__(self,n_users,steps):
        imports()
        preprocess(n_users,steps)

        self.graph = Graph()

        with self.graph.as_default():
            self.sess = Session()

    def compile(self, optimizer, loss, metrics, model):
        K.set_session(self.sess)

        with self.graph.as_default():
            
            #model = decompressBytesToString(model.encode('utf-8'))
            #model = decompressBytesToString(model)
            self.model=decode(model)
            with open('out','a') as stout:
                print(optimizer,loss,metrics,file=stout)

            self.model.compile(optimizer = optimizer , loss = loss , metrics = metrics)

    def fit(self, user_i, step_i, epochs = 1, batch_size = None):
        global X_train_split,y_train_split
        K.set_session(self.sess)

        with self.graph.as_default():
            self.model.fit(X_train_split[user_i][step_i] , y_train_split[user_i][step_i] , epochs = 1 , batch_size = int(batch_size))

    def accuracy_score(self):
        global X_test_flat,y_test
        K.set_session(self.sess)

        with self.graph.as_default():
            y_pred = self.model.predict(X_test_flat)
            y_pred = [np.argmax(i) for i in y_pred]
            accuracy = acc_score(y_test, y_pred)
            return accuracy

    def best_model(self):
        K.set_session(self.sess)

        with self.graph.as_default():
            jsonmodel=encode(self.model)
            #jsonmodel = compressStringToBytes(jsonmodel).decode('utf-8')
            #encmodel = compressStringToBytes(jsonmodel)
            return jsonmodel
            #self.model.save_weights('best_model.h5')

    def update_model(self,model):
        K.set_session(self.sess)

        with self.graph.as_default():
            #model = decompressBytesToString(model.encode('utf-8'))
            #model = decompressBytesToString(model)
            self.model=decode(model)
            #self.model.load_weights('best_model.h5')

def send(topic,data):
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=topic)
    channel.basic_publish(exchange='', routing_key=topic, body=dumps(data))
    connection.close()

if __name__ == '__main__':
    user=None
    
    consumer_conn = pika.BlockingConnection(parameters)
    consumer_channel = consumer_conn.channel()
    consumer_channel.queue_declare(queue=rtopic)
    for method_frame, properties, body in consumer_channel.consume(rtopic):
        # Acknowledge the message
        consumer_channel.basic_ack(method_frame.delivery_tag)
        x=loads(body.decode('utf-8'))
        with open('out','a') as stout:
            print("MSG",x['fun'],file=stout,flush=True)
        if x['fun']=='userinit':
            n_users=x['n_users']
            steps=x['steps']
            user=User(n_users,steps)
        elif x['fun']=='usercompile':
            optimizer=x['optimizer']
            loss=x['loss']
            metrics=x['metrics']
            model=x['model']
            user.compile(optimizer,loss,metrics,model)
        elif x['fun']=='userfit':
            #X_train=np.array(x['X_train'])
            #y_train=np.array(x['y_train'])
            user_i=x['user_i']
            step_i=x['step_i']
            epochs=x['epochs']
            batch_size=x['batch_size']
            user.fit(user_i,step_i,epochs,batch_size)
        elif x['fun']=='accuracyscore':
            accuracy=user.accuracy_score()
            send('w2m',{'accuracy':accuracy})

        elif x['fun']=='bestmodel':
            model=user.best_model()
            send('w2m1',{'model':model})

        elif x['fun']=='bestmodel':
            model=x['model']
            user.update_model(model)

