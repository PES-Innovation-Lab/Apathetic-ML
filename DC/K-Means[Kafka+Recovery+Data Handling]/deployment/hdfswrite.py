from hdfs import InsecureClient
import pandas as pd
client = InsecureClient('http://35.237.123.111:9870','root')
a = open('USA_Housing.csv','r')
client.write('dataset/USA_Housing.csv',data=a)
a.close()
#with open('hdfs://35.188.74.145:8020/user/root/dataset/USA_Housing.csv','r') as reader:
       # df = pd.read_csv(reader)
#print(df)
