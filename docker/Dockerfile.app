FROM python:latest

WORKDIR /project

RUN apt update
RUN apt install libmariadb-dev

RUN pip install --upgrade pip
RUN ls

RUN pip install requests \
                sqlalchemy \
                schedule \
                mariadb \
                pandas