FROM hjben/hbase:1.6.0-hadoop3.3.0
MAINTAINER hjben <hj.ben.kim@gmail.com>

ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV HIVE_HOME /usr/local/hive
ENV HIVE_VERSION 3.1.2
ENV HIVE_CONF_DIR $HIVE_HOME/conf
ENV SQOOP_HOME /usr/local/sqoop
ENV SQOOP_VERSION 1.4.7
ENV SQOOP_HADOOP_VERSION 2.6.0
ENV MARIADB_CONNECTOR_VERSION 2.7.2

# pig
ENV PIG_HOME /usr/local/pig
ENV PIG_CLASSPATH ${PIG_HOME}/conf

# spark
ENV SPARK_HOME /usr/local/spark
ENV SCALA_HOME /usr/local/scala
ENV SCALA_VERSION 2.13.11
ENV SPARK_VERSION 3.4.1

# path
ENV PATH $PATH:$HIVE_HOME/bin:$SQOOP_HOME/bin:${PIG_HOME}/bin:${SCALA_HOME}/bin:${SPARK_HOME}/bin

RUN wget https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz
RUN tar -xzf apache-hive-$HIVE_VERSION-bin.tar.gz -C /usr/local && \
    rm -f apache-hive-$HIVE_VERSION-bin.tar.gz
RUN ln -s /usr/local/apache-hive-$HIVE_VERSION-bin $HIVE_HOME
ADD hive/conf/hive-site.xml $HIVE_HOME/conf/
ADD hive/sh/*sh $HIVE_HOME/bin/
RUN chmod 755 $HIVE_HOME/bin/init-hive-dfs.sh
RUN chmod 755 $HIVE_HOME/bin/stop-server.sh
RUN mkdir $HIVE_HOME/logs

# scoop
RUN wget https://archive.apache.org/dist/sqoop/$SQOOP_VERSION/sqoop-$SQOOP_VERSION.bin__hadoop-$SQOOP_HADOOP_VERSION.tar.gz
RUN tar -xf sqoop-$SQOOP_VERSION.bin__hadoop-$SQOOP_HADOOP_VERSION.tar.gz -C /usr/local && \
    rm -f sqoop-$SQOOP_VERSION.bin__hadoop-$SQOOP_HADOOP_VERSION.tar.gz
RUN ln -s /usr/local/sqoop-$SQOOP_VERSION.bin__hadoop-$SQOOP_HADOOP_VERSION $SQOOP_HOME
ADD sqoop/lib/*.jar $SQOOP_HOME/lib/
ADD hbase/lib/*.jar $SQOOP_HOME/lib/

RUN for lib in client common protocol server; do \
    cp $HBASE_HOME/lib/hbase-${lib}-$HBASE_VERSION.jar $SQOOP_HOME/lib/; \
    done

RUN rm -f $HIVE_HOME/lib/guava-*.jar && cp $HADOOP_HOME/share/hadoop/hdfs/lib/guava-*.jar $HIVE_HOME/lib/
RUN rm -f $SQOOP_HOME/lib/guava-*.jar && cp $HADOOP_HOME/share/hadoop/hdfs/lib/guava-*.jar $SQOOP_HOME/lib/
RUN wget https://downloads.mariadb.com/Connectors/java/connector-java-$MARIADB_CONNECTOR_VERSION/mariadb-java-client-$MARIADB_CONNECTOR_VERSION.jar
RUN cp mariadb-java-client-$MARIADB_CONNECTOR_VERSION.jar $HIVE_HOME/lib/
RUN mv mariadb-java-client-$MARIADB_CONNECTOR_VERSION.jar $SQOOP_HOME/lib/

# PIG
RUN wget https://dlcdn.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz && \
    tar -xf pig-0.17.0.tar.gz && \
    mv pig-0.17.0 ${PIG_HOME} && \
    rm -f pig-0.17.0.tar.gz

# ADD YUM MIRRORS AND UPDATE
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && \
    sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* && \
    yum update -y && \
    yum install -y python36-3.6.8 python3-pip-9.0.3
    RUN pip3 install --upgrade pip && pip3 install numpy pandas scikit-build fuzzywuzzy[speedup]

# SPARK
WORKDIR /tmp

# SCALA (spark dependency)
RUN wget https://downloads.lightbend.com/scala/2.13.11/scala-2.13.11.tgz && \
    tar -xf scala-2.13.11.tgz && \
    rm -f scala-2.13.11.tgz && \
    mv scala-2.13.11/ ${SCALA_HOME}

# SPARK SPARK
RUN wget https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3-scala2.13.tgz && \
    tar -xf spark-3.4.1-bin-hadoop3-scala2.13.tgz && \
    rm -f spark-3.4.1-bin-hadoop3-scala2.13.tgz && \
    mv spark-3.4.1-bin-hadoop3-scala2.13/ ${SPARK_HOME}

# ADD DATA
ADD zbiory/*.csv /zbiory/
ADD zbiory/scripts/*.sh /zbiory/scripts/
EXPOSE 9083 10000 10002