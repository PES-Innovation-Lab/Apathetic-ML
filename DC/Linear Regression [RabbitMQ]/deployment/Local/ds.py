import random

f1 = open('dataset.csv','w')
X = list(range(0,100000,1))
y = [(5 + random.random() - 0.5)*i + 30000 for i in X]
for i in range(len(X)):
    f1.write(str(X[i]) + "," + str(y[i]) + "\n")
f1.close()
