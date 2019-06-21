FROM alpine:3.7

WORKDIR /app

COPY . /app
RUN apk add --no-cache python3 py-pip nfs-utils && pip install --trusted-host pypi.python.org -r requirements.txt

EXPOSE 80

ENV NAME ApatheticML

ENTRYPOINT [ "/start.sh" ]

