from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def init_sparksession(
    name: str, shuffle_partitions: int = None, log_level: str = "WARN"
) -> SparkSession:
    """Initialise SparkSession, the level of log for Spark and some configuration parameters

    Parameters
    ----------
    name: str
        Name for the Spark Application.
    shuffle_partitions: int, optional
        Number of partition to use when shuffling data.
        Typically better to keep the size of shuffles small.
        Default is None.

    Returns
    -------
    spark: SparkSession
        Spark Session initialised.

    Examples
    --------
    >>> spark_tmp = init_sparksession("test")
    >>> conf = spark_tmp.sparkContext.getConf().getAll()
    """
    # Grab the running Spark Session,
    # otherwise create it.
    spark = (
        SparkSession.builder
        .appName(name)
        .config(
            "spark.hadoop.hbase.zookeeper.quorum",
            "hadoop-hbase-cluster"
        )
        .config(
            "spark.hadoop.hbase.zookeeper.property.clientPort",
            "2181"
        )
        .getOrCreate()
    )
    # keep the size of shuffles small
    if shuffle_partitions is not None:
        spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

    # Set spark log level to WARN
    spark.sparkContext.setLogLevel(log_level)

    return spark


def load_parquet_files(path: str) -> DataFrame:
    """Initialise SparkSession, and load parquet files with Spark

    return a standard DataFrame, and not a Streaming DataFrame.

    Parameters
    ----------
    path: str
        The path to the data

    Returns
    -------
    df: DataFrame
        Spark SQL DataFrame

    Examples
    --------
    >>> df = load_parquet_files(ztf_alert_sample)
    """
    # Grab the running Spark Session
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.format("parquet").option("mergeSchema", "true").load(path)

    return df

def list_hdfs_files(hdfs_path):
    """List files on an HDFS folder with full path

    Parameters
    ----------
    hdfs_path: str
        Folder name on HDFS containing files

    Returns
    -------
    paths: list of str
        List of filenames with full path
    """
    spark = SparkSession.builder.getOrCreate()

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()

    Path = jvm.org.apache.hadoop.fs.Path

    root = Path(hdfs_path)
    fs = root.getFileSystem(conf)

    paths = []

    def walk(path):
        for status in fs.listStatus(path):
            current = status.getPath()

            if status.isFile():
                paths.append(current.toString())
            elif status.isDirectory():
                walk(current)

    walk(root)

    return paths