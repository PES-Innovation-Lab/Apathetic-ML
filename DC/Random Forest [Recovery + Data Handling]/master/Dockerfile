FROM 192.168.0.10:8080/rf_v2_master

WORKDIR /app
RUN cd /app && rm -r *
COPY . /app
#RUN pip3 install jsonpickle
#RUN apt-get -y install python3-pip
#RUN apt-get -y install python3-pip
EXPOSE 4000 5000 873 22

ENV NAME ApatheticML

#ENTRYPOINT [ "sh ./start.sh" ]

