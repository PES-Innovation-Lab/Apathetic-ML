FROM 192.168.0.10:8080/lr_v2_master

WORKDIR /app
RUN cd /app && rm -r *
COPY . /app
#RUN pip3 install --trusted-host pypi.python.org -r requirements.txt
#RUN pip3 install kafka-python
#RUN apt-get -y install python3-pip
#RUN apt-get -y install python3-pip
EXPOSE 4000 5000 873 22

ENV NAME ApatheticML
ENV PYTHONUNBUFFERED=0
#ENTRYPOINT [ "sh ./start.sh" ]

