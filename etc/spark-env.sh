#!/usr/bin/env bash

export PYSPARK_PYTHON=/usr/bin/python3.5
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.5
export SPARK_YARN_USER_ENV="PYSPARK_PYTHON=/usr/bin/python3.5"
export PYTHONPATH=$AUT_PATH/target/aut.zip:$PYTHONPATH

