#!/bin/bash
if [ ! -d "$(pwd)/lib/" ]; then
    mkdir $(pwd)/lib/
fi

# Create ENV var for JAVA
if [[ -z "${JAVA_HOME}" ]]; then
    echo "Setting value of JAVA_HOME"
    echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/' >> ~/.bashrc.user

fi

# If directory does not exist, download java
#if [ ! -d "${JAVA_HOME}" ]; then
#    echo "Java not found - downloading"
#    curl -L -b "oraclelicense=a" \
#        "https://download.oracle.com/otn-pub/java/jdk/8u201-b09/42970487e3af4f5aa5bca3f542482c60/jdk-8u201-linux-x64.tar.gz" > ./lib/java.gz
#    
#    tar -xvzf ./lib/java.gz -C ./lib/
#
#    rm ./lib/java.gz
#    
#    echo "Jave download completed"
#fi

# Currently, this runs into error (https://github.com/archivesunleashed/aut/pull/314) not applied to release
# Use the following call instead: [  ./spark-2.4.1-bin-hadoop2.7/bin/spark-shell --packages "io.archivesunleashed:aut:0.17.0"  ]
# Download AUT for faster local-use
if [ ! -d "$(pwd)/lib/aut" ]; then
    echo "Downloading AUT"
    mkdir "$(pwd)/lib/aut"
    curl -L "https://github.com/archivesunleashed/aut/releases/download/aut-0.17.0/aut-0.17.0-fatjar.jar" > "$(pwd)/lib/aut/aut.jar"
fi

SPARK_PATH=$(pwd)/lib/spark-2.4.1-bin-hadoop2.7

# Download spark
if [ ! -d "SPARK_PATH" ]; then
    echo "Downloading Spark"
    curl -L "https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz" > $(pwd)/lib/spark.tgz
    tar -xvf $(pwd)/lib/spark.tgz -C $(pwd)/lib/

    rm $(pwd)/lib/spark.tgz
    echo "Spark download complete"

    # Download missing package to run for pyspark
    curl -L "http://maven.twttr.com/com/hadoop/gplcompression/hadoop-lzo/0.4.20/hadoop-lzo-0.4.20.jar" > "$SPARK_PATH/jars/"
fi

# Change config to local config
if [ ! -d "$SPARK_PATH/yarn_config" ]; then
    cp -r "$YARN_CONF_DIR" "$SPARK_PATH"
    mv "$SPARK_PATH/conf.snap" "$SPARK_PATH/yarn_config"

    # Replace core-site.xml with our own
    cp "$(pwd)/etc/core-site.xml" "$SPARK_PATH/yarn_config/"
    
    # Set env variables for spark
    echo "Setting value of SPARK_VARIABLES"
    echo "export SPARK_HOME=$SPARK_PATH" >> ~/.bashrc.user
    echo 'export SPARK_LOCAL_IP=127.0.0.1' >> ~/.bashrc.user
    echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc.user
    echo "export YARN_CONF_DIR=$SPARK_PATH/yarn_config" >> ~/.bashrc.user
fi

if [ ! -d "$(pwd)/data/" ]; then
    mkdir $(pwd)/data/
fi

# Download sample data
if [ ! -f "$(pwd)/data/example.arc.gz" ]; then
    echo "Downloading sample data"
    curl -L "https://raw.githubusercontent.com/archivesunleashed/aut/master/src/test/resources/arc/example.arc.gz" > $(pwd)/data/example.arc.gz
fi


source ~/.bashrc.user
