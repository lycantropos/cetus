FROM python:latest

COPY . /beylerbey/
WORKDIR /beylerbey/

RUN python setup.py develop
