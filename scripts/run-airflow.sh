#!/bin/bash

python --version

export AIRFLOW_HOME=~/airflow

pip install apache-airflow

sudo apt-get install lsof

PID=$(sudo lsof -t -i:8080)

if [ ! -z "$PID" ]; then
    sudo kill -9 $PID
fi

export PATH=$PATH:~/.local/bin

airflow db init

airflow standalone