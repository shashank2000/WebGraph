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

MAVEN_PATH=$(pwd)/lib/apache-maven-3.6.1

# Download Maven
if [ ! -d "MAVEN_PATH" ]; then
    echo "Installing Maven"
    
    curl -L "http://apache.spinellicreations.com/maven/maven-3/3.6.1/binaries/apache-maven-3.6.1-bin.tar.gz" > $(pwd)/lib/maven.tgz
    tar -xvf $(pwd)/lib/maven.tgz -C $(pwd)/lib/
    rm $(pwd)/lib/maven.tgz
    echo "Maven download complete"

    echo "Setting value of MAVEN_VARIABLES"
    echo "export M2_HOME=$MAVEN_PATH" >> ~/.bashrc.user
    echo 'export PATH=$M2_HOME/bin:$PATH' >> ~/.bashrc.user
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
