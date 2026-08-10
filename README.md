# fink-hbase-experiments

Experimental repository for evaluating and benchmarking different HBase ingestion strategies for the Fink Science archive.

The goal is to compare the performance and resource usage of the standard HBase write path versus HBase Bulk Load using representative Fink datasets.

In this example, we experiment with archiving real ZTF data into HBase.

---

## Setup
- start all containers (hadoop, hbase, hdfs, spark) with : 

```bash 
sudo docker compose up -d 
```

### Copy the dataset into the Hadoop container

From the host machine, copy the test dataset into the Hadoop/HBase container:

```bash
sudo docker cp ./ztf_data_test hadoop-hbase-cluster:/root/
```

Verify that the dataset is available inside the container:

```bash
sudo docker exec -it hadoop-hbase-cluster bash

ls /root/
```

Load the dataset into HDFS, Inside the Hadoop container, create the target HDFS directory, for example:

```bash
hdfs dfs -mkdir -p /archive/science/year=2026/month=08
## set replication factor to 1 
hdfs dfs -setrep -w 1 /archive/science/year=2026/month=08
```

Upload the dataset to HDFS:

```bash
hdfs dfs -put /root/ztf_data_test/* /archive/science/year=2026/month=08/
```

Verify that ~ 4G of data that loaded with sucess in hdfs : 

```bash
hdfs dfs -du -h /archive/science/year=2026/ 
```

Output : 

```text
3.9 G  3.9 G  /archive/science/year=2026/month=08
``` 

--- 
## RUN CLASSIC LOAD WITH 

inside `spark-master` container, run the following command : 

```bash 
/workspace/run_spark.sh /workspace/experiments/archive_science.py \
    hdfs://hadoop-hbase-cluster:9000/archive/science/year=2026/month=08/ \
    classic \
    ztf_main_table \
    ztf_main_table_catalog
```

## RUN BULK LOADING

```bash 
/workspace/run_spark.sh /workspace/experiments/archive_science.py\
    hdfs://hadoop-hbase-cluster:9000/archive/science/year=2026/month=08/ \
    bulk_load \
    ztf_main_table \
    ztf_main_table_catalog \
    --output-path hdfs://hadoop-hbase-cluster:9000/hfiles_tmp
``` 
---

## TODO

* By definition, ThinBulkLoad does not accept two records with the same row key; it requires a unique row key for every record. we need to find a solution for this (for example, by defining a unique row key instead of "objectId_jd," which is not unique) -> FIXED 
* probably take a large dataset from fink (> 4G)

