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
20.9 G  20.9 G  /archive/science/year=2026/month=08
``` 
```text
$ hdfs dfs -du -h /archive/science/year=2026/month=08

7.0 G  7.0 G  /archive/science/year=2026/month=08/day=02
5.4 G  5.4 G  /archive/science/year=2026/month=08/day=03
1.7 G  1.7 G  /archive/science/year=2026/month=08/day=04
2.9 G  2.9 G  /archive/science/year=2026/month=08/day=05
1.6 G  1.6 G  /archive/science/year=2026/month=08/day=06
2.3 G  2.3 G  /archive/science/year=2026/month=08/day=08

``` 

- After this, run this script (to create the salted table with 50 predefined salts): 

```bash
/workspace/create_ztf_table_salted.sh
```


### Generate the jar (for hbase/ java project)

inside `spark-master` container, to build the Java hbase (BulkLoad wrapper) project and generate the JAR file, run:

```bash 
sudo docker exec -it spark-master bash  # first enter spark-master container
cd /workspace/hbase/ && mvn clean package 
``` 
--- 
## RUN CLASSIC LOAD WITH 

inside `spark-master` container, run the following command  : 

- if you want to test the no-salted table replace `ztf_main_table_salted` by `ztf_main_table` 

```bash 
/workspace/run_spark.sh /workspace/experiments/archive_science.py \
    hdfs://hadoop-hbase-cluster:9000/archive/science/year=2026/month=08/ \
    classic \
    ztf_main_table_salted \
    ztf_main_table_catalog \
    --n-partitions 50 
```

## RUN BULK LOADING

- if you want to test the no-salted table replace `ztf_main_table_salted` by `ztf_main_table` 

```bash 
/workspace/run_spark.sh /workspace/experiments/archive_science.py\
    hdfs://hadoop-hbase-cluster:9000/archive/science/year=2026/month=08/ \
    bulk_load \
    ztf_main_table_salted \
    ztf_main_table_catalog \
    --output-path hdfs://hadoop-hbase-cluster:9000/hfiles_tmp \
    --n-partitions 50 
``` 

--- 

## Experiments Results

The main objective of this project is to compare two approaches for loading Spark data into HBase: the **classic load**, based on standard HBase `Put` operations, and the **bulk load**, which consists of generating HFiles from Spark and then loading these HFiles directly into HBase.

The experiments focus specifically on **FINK/ZTF post-science data**, with the goal of determining at what data volume bulk loading becomes more efficient than the classic approach. All experiments were conducted under the **same hardware, Docker, Spark, and HBase configuration**, in order to ensure a fair and consistent comparison. The results presented below therefore focus specifically on the FINK/ZTF use case and the experimental environment used in this project.

### Experimental Setup

Several experiments were conducted to compare (classic vs bulk load). For each experiment, the main metrics were:

1. **Job execution time**, measured using the Spark UI available at `http://localhost:8080`.
2. **Data integrity**, verified after each load using both the `HBase shell` and the --HBase Master UI-- available at `http://localhost:16010`.

The experimental environment runs in Docker Compose on a VM with **16 vCPUs and 32 GB of RAM**.

The experiments were performed with progressively larger Parquets data:

- 2 GB
- 4 GB
- 16 GB
- 20 GB

The objective was to observe how the relative performance of the two approaches evolves as the input data volume increases.

### Results for 2 GB and 4 GB

For the first two experiments (2 GB and 4 GB), the **classic load clearly outperformed the bulk load**. On average, the classic approach was approximately **0.8 minutes faster** than the bulk-loading approach.

At first sight, this result may seem surprising since bulk loading is generally designed for large-scale data ingestion. However, we concluded that, at these relatively small data volumes, the bulk-loading approach is significantly affected by its additional overhead (especially in the startup).

Indeed, bulk loading requires several additional steps, including data repartitioning, the internal sorting performed during HFile generation, and HFile creation. For small datasets, the cost of these operations can outweigh the performance benefits. The final HFile loading phase into HBase, however, is almost instantaneous, since it essentially involves 1 load operation per HFile/region rather than individual writes for each record.

Therefore, these first experiments suggest that **bulk loading is not necessarily advantageous for small data volumes**. To better observe the potential benefits of bulk loading, larger datasets were therefore required.

### Results for 16 GB

The 16 GB experiment started to show a different behavior.

On average:

| Loading method | Average execution time |
|---|---:|
| Classic load | 3.3 min |
| Bulk load | 3.1 min |

The bulk-loading approach was therefore slightly faster with **Speedup ≈ x 1.06**. This corresponds to an improvement of approximately **6.5%** compared with the classic load.

Although the difference is relatively small, this experiment represents an important transition point: the additional overhead of HFile generation becomes less significant compared with the amount of data being processed, and the benefits of bypassing the traditional HBase write path start to compensate for this overhead.

### Results for 20 GB

To confirm this trend, the dataset size was further increased to **20 GB**. The results clearly demonstrate a stronger advantage for bulk loading:

| Loading method | Average execution time |
|---|---:|
| Classic load | 4.7 min |
| Bulk load | 3.6 min |

The **Speedup ≈ 1.31×** -- bulk loading is approximately **23.4% faster** than the classic load for this experiment.

This result confirms the trend observed with the 16 GB dataset: as the data volume increases, the initial overhead associated with bulk loading becomes relatively less important, while the efficiency gained from directly generating and loading HFiles becomes increasingly significant.

### Preliminary Conclusion

Based on the experiments conducted so far, a clear trend can be observed for the **FINK/ZTF datasets in the current experimental environment**. For relatively small datasets (2–4 GB), the classic HBase loading approach performs better because the overhead introduced by the bulk-loading process is not sufficiently amortized. At **20 GB**, the advantage of bulk loading becomes much more significant, with a reduction of about **23.4% in execution time**.

These results suggest that, **for the FINK/ZTF and the tested environment, the transition point is around 16 GB**:

- **< ~16 GB:** classic loading is currently more efficient.
- **~16 GB:** bulk loading starts becoming competitive.
- **> ~16 GB:** bulk loading becomes increasingly attractive as the dataset size grows.

This should not be interpreted as a universal HBase threshold. The exact break-even point depends on the hardware configuration, Spark and HBase settings, cluster size, Hbase tables, row-key distribution, number of regions, and characteristics of the dataset. Nevertheless, it provides a useful empirical guideline for the FINK use case.

This result is particularly relevant for the future **FINK/Rubin data volumes**, where the expected nightly data volume can reach approximately **1 TB per night**. At this scale, the overhead observed for the smaller datasets should become negligible compared with the performance benefits of bulk loading, making the bulk-loading approach a strong candidate for nightly ingestion.

### Bulk Loading and HBase Table Layout

An important observation from the experiments concerns the relationship between **bulk loading performance and HBase table design**.

according to my tests, the bulk-loading approach performs particularly well when the HBase table is **pre-split into regions** and uses a **salted row key**, with the number of salts matching the number of pre-split regions.

In my experiments, the table was configured with:

- **50 pre-split regions**
- **50 salts**
- Then  : `nb_salt = nb_region`
- **1 HFile generated per column_family/per region** during the bulk-loading process

This configuration provides a good alignment between the Spark-side partitioning and the HBase region layout.

Another important observation is that explicitly repartitioning the Spark DataFrame using the row key significantly improves HFile generation:

```python
df = df.repartition(n, "row_key")
```

where n corresponds to the number of HBase regions (and, in these experiments, also to the number of salts = "--n-partition" args = 50 ).

This repartitioning is important because it allows Spark to distribute the data according to the row-key space before generating the HFiles. In the tested configuration, this results in a more direct correspondence between Spark partitions, generated HFiles, and HBase regions, with 1 HFile per region per column family.


Another important remark is that, by design `ThinBulkLoad` requires each record to have a unique row key and therefore does not support multiple records sharing the same row key in the DataFrame. In my experiments, this issue was addressed by defining a unique composite row key, `objectId_jd_candid`, instead of `objectId_jd`, which is not unique.


---
## TODO

- Find a way to clean up (delete) the output path where the HFiles are generated immediately after the execution of `doBulkLoad(...)`. See the code following the `load.doBulkLoad(...)` call in `fink_broker_utils/hbase_utils.py` (line 804).