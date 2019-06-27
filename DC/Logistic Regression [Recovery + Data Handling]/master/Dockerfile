FROM altariax0x01/lr_master_res

WORKDIR /app
RUN cd /app && rm -r *
COPY . /app
#RUN pip3 install --trusted-host pypi.python.org -r requirements.txt
#RUN apt-get -y install python3-pip
#RUN apt-get -y install python3-pip
EXPOSE 4000 5000 873 22

ENV NAME ApatheticML

#ENTRYPOINT [ "sh ./start.sh" ]

