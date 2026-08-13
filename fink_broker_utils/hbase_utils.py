import os
import logging
import numpy as np
import json


import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

BLAZAR_LOW_COLS = ["instantness_low", "robustness_low"]
BLAZAR_HIGH_COLS = ["instantness_high", "robustness_high"]
CDF_COL = ["cdf_quantile"]

MANGROVE_COLS = ["HyperLEDA_name", "2MASS_name", "lum_dist", "ang_dist"]
BLAZAR_COLS = BLAZAR_LOW_COLS + BLAZAR_HIGH_COLS + CDF_COL

_LOG = logging.getLogger(__name__)

def load_fink_cols():
    """Fink-derived columns used in HBase tables with type.

    Returns
    -------
    out: dictionary
        Keys are column names (flattened). Values are data type.

    Examples
    --------
    >>> fink_cols, fink_nested_cols = load_fink_cols()
    >>> print(len(fink_cols))
    34

    >>> print(len(fink_nested_cols))
    9
    """
    fink_cols = {
        "DR3Name": {"type": "string", "default": "Unknown"},
        "Plx": {"type": "float", "default": 0.0},
        "anomaly_score": {"type": "double", "default": 0.0},
        "cdsxmatch": {"type": "string", "default": "Unknown"},
        "e_Plx": {"type": "float", "default": 0.0},
        "gcvs": {"type": "string", "default": "Unknown"},
        "mulens": {"type": "double", "default": 0.0},
        "nalerthist": {"type": "int", "default": 0},
        "rf_kn_vs_nonkn": {"type": "double", "default": 0.0},
        "rf_snia_vs_nonia": {"type": "double", "default": 0.0},
        "roid": {"type": "int", "default": 0},
        "snn_sn_vs_all": {"type": "double", "default": 0.0},
        "snn_snia_vs_nonia": {"type": "double", "default": 0.0},
        "tracklet": {"type": "string", "default": ""},
        "vsx": {"type": "string", "default": "Unknown"},
        "x3hsp": {"type": "string", "default": "Unknown"},
        "x4lac": {"type": "string", "default": "Unknown"},
        "lc_features_g": {"type": "string", "default": "[]"},
        "lc_features_r": {"type": "string", "default": "[]"},
        "jd_first_real_det": {"type": "double", "default": 0.0},
        "jdstarthist_dt": {"type": "double", "default": 0.0},
        "mag_rate": {"type": "double", "default": 0.0},
        "sigma_rate": {"type": "double", "default": 0.0},
        "lower_rate": {"type": "double", "default": 0.0},
        "upper_rate": {"type": "double", "default": 0.0},
        "delta_time": {"type": "double", "default": 0.0},
        "from_upper": {"type": "boolean", "default": False},
        "spicy_id": {"type": "int", "default": -1},
        "spicy_class": {"type": "string", "default": "Unknown"},
        "tns": {"type": "string", "default": ""},
        "gaiaVarFlag": {"type": "int", "default": 0},
        "gaiaClass": {"type": "string", "default": "Unknown"},
        "is_transient": {"type": "boolean", "default": False},
        "slsn_score": {"type": "float", "default": -1},
    }

    fink_nested_cols = {}
    for col_ in MANGROVE_COLS:
        name = "mangrove.{}".format(col_)
        fink_nested_cols.update({name: {"type": "string", "default": "None"}})

    for col_ in BLAZAR_COLS:
        name = "blazar_stats.{}".format(col_)
        fink_nested_cols.update({name: {"type": "float", "default": 0.0}})

    return fink_cols, fink_nested_cols


def load_all_ztf_cols():
    """Fink/ZTF columns used in HBase tables with type.

    Returns
    -------
    out: dictionary
        Keys are column names (flattened). Values are data type.

    Examples
    --------
    >>> root_level, candidates, fink_cols, fink_nested_cols = load_all_ztf_cols()
    >>> out = {**root_level, **candidates, **fink_cols, **fink_nested_cols}
    >>> print(len(out))
    151
    """
    fink_cols, fink_nested_cols = load_fink_cols()

    root_level = {
        "fink_broker_version": "string",
        "fink_science_version": "string",
        "objectId": "string",
        "publisher": "string",
        "candid": "long",
        "schemavsn": "string",
    }

    candidates = {
        "aimage": "float",
        "aimagerat": "float",
        "bimage": "float",
        "bimagerat": "float",
        "chinr": "float",
        "chipsf": "float",
        "classtar": "float",
        "clrcoeff": "float",
        "clrcounc": "float",
        "clrmed": "float",
        "clrrms": "float",
        "dec": "double",
        "decnr": "double",
        "diffmaglim": "float",
        "distnr": "float",
        "distpsnr1": "float",
        "distpsnr2": "float",
        "distpsnr3": "float",
        "drb": "float",
        "drbversion": "string",
        "dsdiff": "float",
        "dsnrms": "float",
        "elong": "float",
        "exptime": "float",
        "fid": "int",
        "field": "int",
        "fwhm": "float",
        "isdiffpos": "string",
        "jd": "double",
        "jdendhist": "double",
        "jdendref": "double",
        "jdstarthist": "double",
        "jdstartref": "double",
        "magap": "float",
        "magapbig": "float",
        "magdiff": "float",
        "magfromlim": "float",
        "maggaia": "float",
        "maggaiabright": "float",
        "magnr": "float",
        "magpsf": "float",
        "magzpsci": "float",
        "magzpscirms": "float",
        "magzpsciunc": "float",
        "mindtoedge": "float",
        "nbad": "int",
        "ncovhist": "int",
        "ndethist": "int",
        "neargaia": "float",
        "neargaiabright": "float",
        "nframesref": "int",
        "nid": "int",
        "nmatches": "int",
        "nmtchps": "int",
        "nneg": "int",
        "objectidps1": "long",
        "objectidps2": "long",
        "objectidps3": "long",
        "pdiffimfilename": "string",
        "pid": "long",
        "programid": "int",
        "programpi": "string",
        "ra": "double",
        "ranr": "double",
        "rb": "float",
        "rbversion": "string",
        "rcid": "int",
        "rfid": "long",
        "scorr": "double",
        "seeratio": "float",
        "sgmag1": "float",
        "sgmag2": "float",
        "sgmag3": "float",
        "sgscore1": "float",
        "sgscore2": "float",
        "sgscore3": "float",
        "sharpnr": "float",
        "sigmagap": "float",
        "sigmagapbig": "float",
        "sigmagnr": "float",
        "sigmapsf": "float",
        "simag1": "float",
        "simag2": "float",
        "simag3": "float",
        "sky": "float",
        "srmag1": "float",
        "srmag2": "float",
        "srmag3": "float",
        "ssdistnr": "float",
        "ssmagnr": "float",
        "ssnamenr": "string",
        "ssnrms": "float",
        "sumrat": "float",
        "szmag1": "float",
        "szmag2": "float",
        "szmag3": "float",
        "tblid": "long",
        "tooflag": "int",
        "xpos": "float",
        "ypos": "float",
        "zpclrcov": "float",
        "zpmed": "float",
    }

    candidates = {"candidate." + k: v for k, v in candidates.items()}

    return root_level, candidates, fink_cols, fink_nested_cols

def cast_features(df):
    """Cast feature columns into string of array

    Parameters
    ----------
    df: Spark DataFrame
        DataFrame of alerts

    Returns
    -------
    df: Spark DataFrame

    Examples
    --------
    # Read alert from the raw database
    >>> df = spark.read.format("parquet").load(ztf_alert_sample_scidatabase)

    >>> df = cast_features(df)
    >>> assert 'lc_features_g' in df.columns, df.columns

    >>> a_row = df.select('lc_features_g').limit(1).toPandas().to_numpy()[0][0]
    >>> assert isinstance(a_row, str), a_row
    """
    if ("lc_features_g" in df.columns) and ("lc_features_r" in df.columns):
        df = df.withColumn("lc_features_g", F.array("lc_features_g.*").astype("string"))

        df = df.withColumn("lc_features_r", F.array("lc_features_r.*").astype("string"))

    return df

def flatten_dataframe(df, root_level, section, fink_cols, fink_nested_cols):
    """Flatten DataFrame columns of a nested Spark DF for HBase ingestion

    Notes
    -----
    Check also all Fink columns exist, fill if necessary, and cast all columns.

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame with raw alert data
    root_level: dict
        Dictionary with root level columns
    section: dict
        Dictionary with nested level columns.
        For ZTF, this will be `candidates`.
        For Rubin, this will be `diaSource` or `diaObject`
    fink_cols: dict
        Dictionary with Fink root level columns
    fink_nested_cols: dict
        Dictionary with Fink nested columns

    Returns
    -------
    df: DataFrame
        Spark DataFrame with HBase data structure
    col_i: list
        List of columns for i column family
    col_d: list
        List of columns for d column family
    cf: dict
        Dictionary with keys being column names (also called
        column qualifiers), and the corresponding column family.
    """
    tmp_i = []
    tmp_d = []

    # assuming no missing columns
    for colname, coltype in root_level.items():
        tmp_i.append(F.col(colname).cast(coltype))

    # assuming no missing columns
    for colname, coltype in section.items():
        tmp_i.append(F.col(colname).cast(coltype).alias(colname.split(".")[-1]))

    cols_i = df.select(tmp_i).columns

    # check all columns exist, otherwise create it
    for colname, coltype_and_default in fink_cols.items():
        try:
            # ony here to check if the column exists
            df.select(colname)
        except AnalysisException:
            _LOG.warn("Missing columns detected in the DataFrame: {}".format(colname))
            _LOG.warn(
                "Adding a new column with value `{}` and type `{}`".format(
                    coltype_and_default["default"], coltype_and_default["type"]
                )
            )
            df = df.withColumn(colname, F.lit(coltype_and_default["default"]))
        tmp_d.append(F.col(colname).cast(coltype_and_default["type"]))

    # check all columns exist, otherwise create it
    for colname, coltype_and_default in fink_nested_cols.items():
        try:  # noqa: PERF203
            # ony here to check if the column exists
            df.select(colname)

            # rename root.level into root_level
            name = (
                F
                .col(colname)
                .alias(colname.replace(".", "_"))
                .cast(coltype_and_default["type"])
            )
            tmp_d.append(name)
        except AnalysisException:  # noqa: PERF203
            _LOG.warn("Missing columns detected in the DataFrame: {}".format(colname))
            _LOG.warn(
                "Adding a new column with value `{}` and type `{}`".format(
                    coltype_and_default["default"], coltype_and_default["type"]
                )
            )
            name = colname.replace(".", "_")
            df = df.withColumn(name, F.lit(coltype_and_default["default"]))
            tmp_d.append(F.col(name).cast(coltype_and_default["type"]))

    cols_d = df.select(tmp_d).columns

    # flatten names
    cnames = tmp_i + tmp_d
    df = df.select(cnames)

    cf = assign_column_family_names(df, cols_i, cols_d)

    return df, cols_i, cols_d, cf

def assign_column_family_names(df, cols_i, cols_d):
    """Assign a column family name to each column qualifier.

    There are currently 2 column families:
        - i: for column that identify the alert (original alert)
        - d: for column that further describe the alert (Fink added value)

    The split is done in `flatten_dataframe`.

    Parameters
    ----------
    df: DataFrame
        Input DataFrame containing alert data from the raw science DB (parquet).
        See `load_parquet_files` for more information.
    cols_*: list of string
        List of DataFrame column names to use for the science portal.

    Returns
    -------
    cf: dict
        Dictionary with keys being column names (also called
        column qualifiers), and the corresponding column family.

    """
    cf = {i: "i" for i in df.select(["`{}`".format(k) for k in cols_i]).columns}
    cf.update({i: "d" for i in df.select(["`{}`".format(k) for k in cols_d]).columns})

    return cf

def add_row_key(df, row_key_name, cols=None):
    """Create and attach the row key to a DataFrame

    This should be typically called before `select_relevant_columns`.

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame
    row_key_name: str
        Row key name (typically columns separated by _)
    cols: list
        List of columns to concatenate (typically split row_key_name)

    Returns
    -------
    out: DataFrame
        Original Spark DataFrame with a new column

    Examples
    --------
    # Read alert from the raw database
    >>> df = spark.read.format("parquet").load(ztf_alert_sample_scidatabase)

    # Flatten columns
    >>> df = df.select(["objectId", "candidate.jd", "candidate.ra"])

    >>> df2 = add_row_key(df, None, None)
    >>> extra_cols = [col for col in df2.columns if col not in df.columns]
    >>> assert len(extra_cols) == 0, "Found {}".format(extra_cols)

    >>> rowkey = "objectId"
    >>> df2 = add_row_key(df, rowkey, rowkey.split("_"))
    >>> extra_cols = [col for col in df2.columns if col not in df.columns]
    >>> assert len(extra_cols) == 0, "Found {}".format(extra_cols)

    >>> rowkey = "objectId_jd"
    >>> df2 = add_row_key(df, rowkey, rowkey.split("_"))
    >>> extra_cols = [col for col in df2.columns if col not in df.columns]
    >>> assert extra_cols == [rowkey], "Found {}".format(extra_cols)

    >>> rowkey = "objectId_objectId"
    >>> df2 = add_row_key(df, rowkey, rowkey.split("_")) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    AssertionError: You have duplicated fields in your columns definition: ['objectId', 'objectId']

    >>> rowkey = "objectId_toto"
    >>> df2 = add_row_key(df, rowkey, rowkey.split("_")) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    AssertionError: Cannot build the rowkey: toto is not in DataFrame Columns ['objectId', 'jd', 'ra']
    """
    if not isinstance(cols, list):
        # should never happen in practice
        return df

    if len(cols) == 1:
        # single field rowkey
        return df

    # check all fields exist
    msg = "Cannot build the rowkey: {} is not in DataFrame Columns {}"
    for col in cols:
        assert col in df.columns, msg.format(col, df.columns)

    # check there is no duplicates
    msg = "You have duplicated fields in your columns definition: {}".format(cols)
    assert len(np.unique(cols)) == len(cols), msg

    row_key_col = F.concat_ws("_", *cols).alias(row_key_name)
    df = df.withColumn(row_key_name, row_key_col)

    return df

def add_salted_row_key(df, row_key_name, cols, num_salts=50):
    original_rowkey = F.concat_ws("_", *cols)

    salt = F.pmod(
        F.xxhash64(original_rowkey),
        F.lit(num_salts)
    )

    salted_rowkey = F.concat(
        F.lpad(salt.cast("string"), 2, "0"),
        F.lit("_"),
        original_rowkey
    )

    return df.withColumn(row_key_name, salted_rowkey)

def select_relevant_columns(
    df: DataFrame, cols: list, row_key_name: str, to_create=None
) -> DataFrame:
    """Select columns from `cols` that are actually in `df`.

    It would act as if `df.select(cols, skip_unknown_cols=True)` was possible. Note though
    that nested cols in `cols` will be flatten, and columns used in `to_create` have to be
    in this list of flatten names. Example, if my initial df has schema
    root
    |-- objectId: string (nullable = true)
    |-- candidate: struct (nullable = true)
    |    |-- jd: double (nullable = true)

    then `to_create` can be `F.col('objectId') + F.col('jd')` but
    not `F.col('objectId') + F.col('candidate.jd')`

    Parameters
    ----------
    df: DataFrame
        Input Spark DataFrame
    cols: list
        Column names to select
    row_key_name: str
        Row key name
    to_create: list
        Extra columns to create from others, and to include in the `select`.
        Example: df.select(['a', 'b', F.col('a') + F.col('c')])

    Returns
    -------
    df: DataFrame

    Examples
    --------
    >>> import pyspark.sql.functions as F
    >>> df = spark.createDataFrame([{'a': 1, 'b': 2, 'c': 3}])

    >>> select_relevant_columns(df, ['a'], '')
    DataFrame[a: bigint]

    >>> select_relevant_columns(df, ['a', 'b', 'c'], '')
    DataFrame[a: bigint, b: bigint, c: bigint]

    >>> select_relevant_columns(df, ['a', 'd'], '')
    DataFrame[a: bigint]

    >>> select_relevant_columns(df, ['a', 'b'], 'c', to_create=[F.col('a') + F.col('b')])
    DataFrame[a: bigint, b: bigint, c: bigint, (a + b): bigint]
    """
    # Add the row key to the list of columns to extract
    all_cols = cols + [row_key_name]

    if (to_create is not None) and isinstance(to_create, list):
        for extra_col in to_create:
            all_cols += [extra_col]

    cnames = []
    missing_cols = []
    for col_ in all_cols:
        # Dumb but simple
        try:  # noqa: PERF203
            df.select(col_)
            cnames.append(col_)
        except AnalysisException:  # noqa: PERF203
            missing_cols.append(col_)

    # flatten names
    df = df.select(cnames)

    _LOG.info("Missing columns detected in the DataFrame: {}".format(missing_cols))

    return df

def construct_hbase_catalog_from_flatten_schema(
    schema: dict, catalogname: str, rowkeyname: str, cf: dict
) -> str:
    """Convert a flatten DataFrame schema into a HBase catalog.

    From
    {'name': 'schemavsn', 'type': 'string', 'nullable': True, 'metadata': {}}

    To
    'schemavsn': {'cf': 'i', 'col': 'schemavsn', 'type': 'string'},

    Parameters
    ----------
    schema : dict
        Schema of the flatten DataFrame.
    catalogname : str
        Name of the HBase catalog.
    rowkeyname : str
        Name of the rowkey in the HBase catalog.
    cf: dict
        Dictionary with keys being column names (also called
        column qualifiers), and the corresponding column family.
        See `assign_column_family_names`.

    Returns
    -------
    catalog : str
        Catalog for HBase.
    """
    schema_columns = schema.jsonValue()["fields"]

    catalog = "".join("""
    {{
        'table': {{
            'namespace': 'default',
            'name': '{}'
        }},
        'rowkey': '{}',
        'columns': {{
    """).format(catalogname, rowkeyname)

    sep = ","
    for column in schema_columns:
        # Last entry should not have comma (malformed json)
        # if schema_columns.index(column) != len(schema_columns) - 1:
        #     sep = ","
        # else:
        #     sep = ""

        # Deal with array
        if isinstance(column["type"], dict):
            column["type"] = "string"

        if column["type"] == "timestamp":
            column["type"] = "string"

        if column["type"] == "boolean":
            column["type"] = "string"

        if column["name"] == rowkeyname:
            catalog += """
            '{}': {{'cf': 'rowkey', 'col': '{}', 'type': '{}'}}{}
            """.format(column["name"], column["name"], column["type"], sep)
        else:
            catalog += """
            '{}': {{'cf': '{}', 'col': '{}', 'type': '{}'}}{}
            """.format(
                column["name"], cf[column["name"]], column["name"], column["type"], sep
            )

    # Push an empty column family 'a' for later annotations
    catalog += "'annotation': {'cf': 'a', 'col': '', 'type': 'string'}"
    catalog += """
        }
    }
    """

    return catalog.replace("'", '"')

def push_to_hbase_partial(hbase_catalog, newtable):
    """Wrapper around Hbase ingestion

    Parameters
    ----------
    hbase_catalog: str
        HBase catalog as a string.
    newtable: int
        Number of region to set if a new table needs
        to be created.

    Returns
    -------
    out: function
        Function to ingest static data
    """

    def inwrap(batch_df, batch_id) -> None:
        """Ingest static data to HBase table

        Parameters
        ----------
        batch_df: Spark DataFrame
            Static Spark DataFrame
        batch_id: int
            ID of the batch (used only for streaming)
        """
        batch_df.write.options(catalog=hbase_catalog, newtable=newtable).format(
            "org.apache.hadoop.hbase.spark"
        ).option("hbase.spark.use.hbasecontext", False).save()

    return inwrap



def convert_catalog_for_java(hbase_catalog, jvm):
    """
    Convert an HBase catalog from a JSON string to a Java HashMap.
    Args:
        hbase_catalog (str): HBase catalog serialized as a JSON string.
        jvm: Py4J JVM gateway associated with the Spark context.

    Returns:
        java.util.HashMap: Java map containing the HBase column definitions,
        including the rowkey definition.

        rowkey : rowKey representation for java
    """
    try:
        catalog = json.loads(hbase_catalog)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "Invalid HBase catalog: expected a valid JSON string."
        ) from exc

    if "columns" not in catalog:
        raise ValueError(
            "Invalid HBase catalog: missing 'columns' field."
        )

    if "rowkey" not in catalog:
        raise ValueError(
            "Invalid HBase catalog: missing 'rowkey' field."
        )

    columns = catalog["columns"]
    rowkey = catalog["rowkey"]

    if not isinstance(rowkey, str) or not rowkey:
        raise ValueError(
            "Invalid HBase catalog: 'rowkey' must be a non-empty string."
        )

    # The rowkey must be present in the column definitions.
    if rowkey not in columns:
        columns[rowkey] = {
            "cf": "rowkey",
            "col": rowkey,
            "type": "string",
        }

    java_columns = jvm.java.util.HashMap()

    for col_name, meta in columns.items():
        if col_name =="annotation" : continue 

        java_meta = jvm.java.util.HashMap()

        java_meta.put("cf", meta["cf"])
        java_meta.put("col", meta["col"])
        java_meta.put("type", meta.get("type", "string"))

        java_columns.put(col_name, java_meta)

    return java_columns, rowkey
def push_to_hbase_bulk_load(sc, df, table_name, hbase_catalog, output_path,n):
    """
    Perform a bulk load of Spark DataFrame data into HBase.

    This function follows the official HBase Spark integration workflow:
        1. Generate HFiles from the DataFrame using the hbaseBulkLoadThinRows Java wrapper.
        2. Load the generated HFiles into HBase using LoadIncrementalHFiles.

    Args:
        sc (SparkSession): Current Spark session.
        df (DataFrame): Spark DataFrame containing the data to load into HBase.
        table_name (str): Name of the target HBase table. The table must
            already exist.
        hbase_catalog (str): HBase catalog configuration as a string.
        output_path (str): HDFS or local path where the generated HFiles
            will be stored.
        n (int) : (Optional) number of DataFrame partitions to consider

    Returns:
        None
    """

    jvm = sc._jvm

    # Java classes
    TableName = jvm.org.apache.hadoop.hbase.TableName

    # HFiles must be sorted by rowkey
    # ThinBulkLoad handles the required sorting internally
    # 1 - Avoid global orderBy() as it triggers an expensive shuffle
    #df = df.orderBy("key")
    # OR  2- Using repartition(n, "key") to control parallelism and HFile generation
    # df = df.repartition(n_partitions, "key")
    
    java_columns, row_key = convert_catalog_for_java(hbase_catalog,jvm)
    
    if n :
        # considere partitioning  
        df = df.repartition(n,row_key)

    # Initialize HBase context
    hbase_conf = sc._jsc.hadoopConfiguration()
    hbase_context = jvm.org.apache.hadoop.hbase.spark.HBaseContext(sc._jsc.sc(), hbase_conf, None)
    
    # Call my Java wrapper
    wrapper = (
        jvm.hbase.ThinBulkLoadWrapper
    )
    # Execute Thin Bulk Load (generate HFiles in "thin")
    wrapper.bulkLoadThinRows(
        hbase_context,
        df._jdf,
        java_columns,
        row_key,
        table_name,

        # Temporary directory where HFiles will be generated (staging area before loading into HBase)
        output_path,

        # HFile write options per column family (compression,bloom filters, block size, etc.)
        # Empty HashMap = default HBase settings
        jvm.java.util.HashMap(),

        # compactionExclude flag: True  -> HFiles are excluded from compactions , False -> normal HBase compaction behavior
        False,

        # Maximum HFile size in bytes (here: 256 MB)
        256 * 1024 * 1024
    )


    # Load generated HFiles into HBase

    connection_factory = jvm.org.apache.hadoop.hbase.client.ConnectionFactory
    conn = connection_factory.createConnection(hbase_conf)

    admin = conn.getAdmin()

    if not admin.tableExists(TableName.valueOf(table_name)):
        raise ValueError(f"Table {table_name} does not exist in HBase!")

    table = conn.getTable(TableName.valueOf(table_name))
    region_locator = conn.getRegionLocator(TableName.valueOf(table_name))

    load = jvm.org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles(hbase_conf)

    load.doBulkLoad(
        jvm.org.apache.hadoop.fs.Path(output_path),
        admin,
        table,
        region_locator
    )

def push_to_hbase(
    df,
    table_name,
    rowkeyname,
    cf,
    nregion=50,
    bulk_loading=False,
    sc=None,
    output_path=None,
    n=None
):
    """Push DataFrame data to HBase

    Parameters
    ----------
    df: Spark DataFrame
        Spark DataFrame
    table_name: str
        Name of the table in HBase
    rowkeyname: str
        Name of the rowkey in the table
    cf: dict
        Dictionnary containing column names with column family
    nregion: int, optional
        Number of region to create if the table is newly created. Default is 50.
    bulk_loading : boolean, optional 
        True : Bulk loading, False : classic load 
    sc : SparkSession, optional
        current spark session
    output_path : str, required for bulk load mode
        HDFS/local path where HFiles will be generated
    n : int, (Optional) number of DataFrame partitions to consider


    """

    # Push the alert data to HBase
    hbcatalog_index = construct_hbase_catalog_from_flatten_schema(
        df.schema, table_name, rowkeyname=rowkeyname, cf=cf
    )

    if bulk_loading:
        push_to_hbase_bulk_load(sc,df,table_name,hbcatalog_index,output_path,n)
    else:
        push_to_hbase_partial(hbcatalog_index, nregion)(df, None)

    return None


def push_full_df_to_hbase(df, row_key_name, table_name, catalog_name, bulk_loading=False, sc=None, output_path=None,n=None):
    """Push data stored in a Spark DataFrame into HBase

    It assumes the main ZTF table schema

    Parameters
    ----------
    df: Spark DataFrame
        Spark DataFrame (full alert schema)
    row_key_name: str
        Name of the rowkey in the table. Should be a column name
        or a combination of column separated by _ (e.g. jd_objectId).
    table_name: str
        HBase table name. If it does not exist, it will
        be created.
    catalog_name: str
        Name for the JSON catalog (saved locally for inspection)
    bulk_loading: bool
        If True, ingest data using bulkLoading strategy.Default is False (classic load).
    sc : SparkSession, optional
        current spark session
    output_path : str, required for bulk load mode
        HDFS/local path where HFiles will be generated
    n : int, (Optional) number of DataFrame partitions to consider
    """
    # Cast feature columns
    df_casted = cast_features(df)

    # Load columns
    root_level, candidates, fink_cols, fink_nested_cols = load_all_ztf_cols()

    # Check all columns exist, fill if necessary, and cast data
    df_flat, cols_i, cols_d, cf = flatten_dataframe(
        df_casted, root_level, candidates, fink_cols, fink_nested_cols
    )

    #df_flat = add_row_key(
    #    df_flat, row_key_name=row_key_name, cols=row_key_name.split("_")
    #)
    
    df_flat = add_salted_row_key(
        df_flat, row_key_name=row_key_name, cols=row_key_name.split("_"), num_salts=50
    )
    # Flatten columns
    df_flat = select_relevant_columns(
        df_flat,
        row_key_name=row_key_name,
        cols=cols_i + cols_d,
    )

    push_to_hbase(
        df=df_flat,
        table_name=table_name,
        rowkeyname=row_key_name,
        cf=cf,
        bulk_loading=bulk_loading,
        sc=sc,
        output_path=output_path,
        n=n
    )