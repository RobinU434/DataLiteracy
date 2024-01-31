FROM ubuntu:latest

WORKDIR /datalit

RUN apt update

RUN apt install -y  apt-utils  \
                    python3 \
                    python3-pip \
                    libmariadb-dev

RUN pip install --upgrade pip

RUN pip install numpy \
                requests \
                schedule \
                pandas \
                pyaml \
                wget \
                shapely \
                geopandas \
                matplotlib \
                tqdm \
                urllib3 \
                scikit-learn \
                pyarrow \
                tueplots \
                seaborn \
                sqlalchemy \
                mariadb
                
                
                