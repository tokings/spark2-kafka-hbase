#!/usr/bin/env bash

MASTER=yarn
DEPLOY_MODE=cluster
#DEPLOY_MODE=client
MAIN_NAME=StreamingToHBaseApp
MAIN_CLASS=com.hncy58.spark2.hbase.StreamingToHBaseApp
PROJECT=spark2
VERSION=spark2-0.0.1-SNAPSHOT-jar-with-dependencies

BATCH_DURATION=10

KAFKA_SERVERS=192.168.144.128:9092
KAFKA_TOPICS=test-topic-1
KAFKA_GROUP=KafkaToHBaseGroup

cd `dirname $0`

MAIN_JAR="./lib/$PROJECT-$VERSION.jar"
PATH_LIB=./lib
JARS=`ls $PATH_LIB/*.jar | head -1`

for jar in `ls $PATH_LIB/*.jar | grep -v $PROJECT | grep -v $JARS`
do
  JARS="$JARS,""$jar"
done

set -x

appId=`yarn application -list | grep $MAIN_NAME | awk '{print $1}'`
for id in $appId
do
   echo "kill app "$id
   yarn application -kill $id
done

nohup spark2-submit \
--name $MAIN_NAME \
--class $MAIN_CLASS \
--master $MASTER     \
--files ./log4j.properties  \
--driver-java-options "-Dlog4j.configuration=./log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dappname=$MAIN_NAME -Dlog4j.configuration=./log4j.properties"   \
--conf "spark.default.parallelism=36"   \
--conf "spark.streaming.concurrentJobs=10"   \
--conf "spark.sql.shuffle.partitions=100"   \
--conf "spark.yarn.executor.memoryOverhead=1024"   \
--conf "spark.streaming.blockInterval=10000ms"   \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"   \
--conf "spark.scheduler.mode=FAIR"   \
--deploy-mode $DEPLOY_MODE     \
--driver-memory 512m     \
--num-executors 2 \
--executor-cores 1     \
--executor-memory 512m     \
--queue default     \
--jars $JARS $MAIN_JAR $KAFKA_SERVERS $KAFKA_TOPICS $MAIN_NAME $MASTER $BATCH_DURATION 1>out.log 2>err.log &