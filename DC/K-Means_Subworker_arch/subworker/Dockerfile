FROM 192.168.0.10:8080/km_v3_worker

WORKDIR /app
RUN cd /app && rm -r *
COPY . /app
#RUN apk update && apk add make automake gcc g++ subversion python3-dev python-dev python3 py-pip nfs-utils && pip3 install --trusted-host pypi.python.org -r requirements.txt
#RUN apt-get -y install python3-pip
#RUN pip3 install --trusted-host pypi.python.org -r requirements.txt
#RUN chmod +x ./start.sh
EXPOSE 4000 5000 22 873

ENV NAME ApatheticML
#CMD ["python3", "LR(Worker).py"]
#ENTRYPOINT [ "start.sh" ]
