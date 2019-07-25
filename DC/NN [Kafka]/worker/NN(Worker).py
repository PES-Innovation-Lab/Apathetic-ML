import socket
from kafka import KafkaConsumer
#from keras import *
import gzip
from io import StringIO, BytesIO
from json import dumps,loads
def decompressBytesToString(inputBytes):
  """
  decompress the given byte array (which must be valid
  compressed gzip data) and return the decoded text (utf-8).
  """
  bio = BytesIO()
  stream = BytesIO(inputBytes)
  decompressor = gzip.GzipFile(fileobj=stream, mode='r')
  while True:  # until EOF
    chunk = decompressor.read(8192)
    if not chunk:
      decompressor.close()
      bio.seek(0)
      return bio.read().decode("utf-8")
    bio.write(chunk)
  return None

def compressStringToBytes(inputString):
  """
  read the given string, encode it in utf-8,
  compress the data and return it as a byte array.
  """
  bio = BytesIO()
  bio.write(inputString.encode("utf-8"))
  bio.seek(0)
  stream = BytesIO()
  compressor = gzip.GzipFile(fileobj=stream, mode='w')
  while True:  # until EOF
    chunk = bio.read(8192)
    if not chunk:  # EOF?
      compressor.close()
      return stream.getvalue()
    compressor.write(chunk)
def imports():
    global np,Dense,tf,K,model_from_json,load_model,Graph,Session,acc_score,KafkaProducer,encode,decode,producer
    import numpy as np
    from keras.layers import Dense
    import tensorflow as tf
    from keras import backend as K
    from keras.models import model_from_json
    from keras.models import load_model
    from tensorflow import Graph,Session
    from kafka import KafkaProducer
    #from json import dumps,loads
    from sklearn.metrics import accuracy_score as acc_score
    from jsonpickle import encode,decode

    tf.reset_default_graph()

    producer = KafkaProducer(acks=True,value_serializer=lambda v: dumps(v).encode('utf-8'),bootstrap_servers = ['kafka-service:9092'])


this_host = socket.gethostname()
myid = this_host[:this_host.find("-")]
myid = myid[-1]

rtopic='m2w'+myid   #topic that worker consumes from

with open("out",'a') as standardout:
    print("NN Launched",rtopic,file=standardout)

class User:

    def __init__(self):
        imports()

        self.graph = Graph()

        with self.graph.as_default():
            self.sess = Session()

    def compile(self, optimizer, loss, metrics, model):
        K.set_session(self.sess)

        with self.graph.as_default():
            '''
            json_file = open('model.json', 'r')
            loaded_model_json = json_file.read()
            json_file.close()
            loaded_model = model_from_json(loaded_model_json)
            loaded_model.load_weights("model.h5")
            self.model = loaded_model
            '''
            model = decompressBytesToString(model.decode('utf-8'))
            self.model=decode(model)
            with open('out','a') as stout:
                print(optimizer,loss,metrics,file=stout)

            self.model.compile(optimizer = optimizer , loss = loss , metrics = metrics)

    def fit(self, X_train, y_train, epochs = 1, batch_size = None):
        K.set_session(self.sess)

        with self.graph.as_default():
            self.model.fit(X_train , y_train , epochs = 1 , batch_size = int(batch_size))

    def accuracy_score(self, X_test, y_test):
        K.set_session(self.sess)

        with self.graph.as_default():
            y_pred = self.model.predict(X_test)
            y_pred = [np.argmax(i) for i in y_pred]
            accuracy = acc_score(y_test, y_pred)
            return accuracy

    def best_model(self):
        K.set_session(self.sess)

        with self.graph.as_default():
            encmodel = compressStringToBytes(encode(self.model)).encode('utf-8')
            return encmodel
            #self.model.save_weights('best_model.h5')

    def update_model(self,model):
        K.set_session(self.sess)

        with self.graph.as_default():
            model = decompressBytesToString(model.decode('utf-8'))
            self.model=decode(model)
            #self.model.load_weights('best_model.h5')


if __name__ == '__main__':
    user=None
    consumer = KafkaConsumer(rtopic,bootstrap_servers=['kafka-service:9092'],group_id=rtopic,auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for msg in consumer:
        x=msg.value
        with open('out','a') as stout:
            print("MSG",x['fun'],file=stout,flush=True)
        if x['fun']=='userinit':
            user=User()
        elif x['fun']=='usercompile':
            optimizer=x['optimizer']
            loss=x['loss']
            metrics=x['metrics']
            model=x['model']
            user.compile(optimizer,loss,metrics,model)
        elif x['fun']=='userfit':
            X_train=np.array(x['X_train'])
            y_train=np.array(x['y_train'])
            epochs=x['epochs']
            batch_size=x['batch_size']
            user.fit(X_train,y_train,epochs,batch_size)
        elif x['fun']=='accuracyscore':
            X_test=np.array(x['X_test'])
            y_test=np.array(x['y_test'])
            accuracy=user.accuracy_score(X_test,y_test)
            producer.send('w2m',{'accuracy':accuracy})
        elif x['fun']=='bestmodel':
            model=user.best_model()
            producer.send('w2m1',{'model':model})
        elif x['fun']=='bestmodel':
            model=x['model']
            user.update_model(model)
