FROM 192.168.0.10:8080/lr_v2_controller

WORKDIR /app

COPY . /app
RUN pip3 install kafka-python
#RUN apt-get -y install rsync ssh
#RUN apt-get -y install python3-pip
EXPOSE 4000 5000 22 873

ENV NAME ApatheticML

#ENTRYPOINT [ "sh ./start.sh" ]

