#!/bin/bash

set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 <python_file>"
    exit 1
fi

PYTHON_FILE=$1
shift

LIB_DIR=/opt/spark/jars/hbase

PROJECT_JARS="$LIB_DIR/hbase-spark-hbase2.5.8_spark3.4.3_scala2.12.0_hadoop3.3.6.jar,\
$LIB_DIR/hbase-spark-protocol-shaded-hbase2.5.8_spark3.4.3_scala2.12.0_hadoop3.3.6.jar,\
/workspace/mapreduce/target/mapreduce-1.0-SNAPSHOT.jar"

echo "Launching Spark job..."

$SPARK_HOME/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 8G \
  --executor-cores 8 \
  --jars "$PROJECT_JARS" \
  --packages "org.apache.hbase:hbase-shaded-mapreduce:2.5.8,\
org.slf4j:slf4j-log4j12:1.7.36,\
org.slf4j:slf4j-simple:1.7.36" \
  "$PYTHON_FILE" "$@"