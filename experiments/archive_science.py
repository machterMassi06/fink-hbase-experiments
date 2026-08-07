import argparse
from fink_broker_utils.spark_utils import init_sparksession, load_parquet_files
from fink_broker_utils.hbase_utils import push_full_df_to_hbase

def getargs():
    parser = argparse.ArgumentParser(
        description="Archive Fink Science data into HBase"
    )

    parser.add_argument(
        "folder",
        help="Path to parquet files"
    )

    parser.add_argument(
        "type",
        help="Load strategy (bulk_loading, classic)"
    )

    parser.add_argument(
        "science_db_name",
        help="HBase table name"
    )

    parser.add_argument(
        "science_db_catalogs",
        help="HBase catalog name"
    )

    return parser.parse_args()

def main():
    args = getargs()

    # Initialise Spark session
    spark = init_sparksession(
        name="science_archival_{}".format(args.type), shuffle_partitions=2
    )

    # Row key
    row_key_name = "objectId_jd"

    print("Processing {}".format(args.folder))

    df = load_parquet_files(args.folder)
    n_alerts = df.count()

    # Drop unused partitioning columns
    df = df.drop("year").drop("month").drop("day")

    # Drop images
    df = df.drop("cutoutScience").drop("cutoutTemplate").drop("cutoutDifference")

    # push data to HBase
    push_full_df_to_hbase(
        df,
        row_key_name=row_key_name,
        table_name=args.science_db_name,
        catalog_name=args.science_db_catalogs,
        bulk_loading=False
    )

    print("{} alerts pushed to HBase".format(n_alerts))


if __name__ == "__main__":
    main()