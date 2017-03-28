FROM python:3

COPY . /cetus/
WORKDIR /cetus/

RUN python setup.py develop
