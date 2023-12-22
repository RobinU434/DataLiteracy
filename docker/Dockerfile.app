FROM ubuntu:latest

WORKDIR /datalit

RUN apt update

RUN apt install -y apt-utils 
RUN apt install -y python3
RUN apt install -y python3-pip

RUN apt install -y libmariadb-dev

RUN pip install --upgrade pip

RUN pip install requests \
                sqlalchemy \
                schedule \
                mariadb \
                pyaml\
                pandas