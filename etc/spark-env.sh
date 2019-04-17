#!/usr/bin/env bash

export PYSPARK_PYTHON=/usr/bin/python3.5
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.5
export SPARK_YARN_USER_ENV="PYSPARK_PYTHON=/usr/bin/python3.5"
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$AUT_PATH/target/aut.zip:$PYTHONPATH

