FROM python:3

COPY . /cetus/
WORKDIR /cetus/

RUN python3 setup.py develop
