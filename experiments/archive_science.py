import argparse

from fink_broker_utils.spark_utils import init_sparksession, load_parquet_files
from fink_broker_utils.hbase_utils import push_full_df_to_hbase



def getargs():
    parser = argparse.ArgumentParser(
        description="Archive Fink Science data into HBase"
    )

    parser.add_argument(
        "folder",
        help="Path to parquet files",
    )

    parser.add_argument(
        "type",
        choices=["bulk_load", "classic"],
        help="Load strategy",
    )

    parser.add_argument(
        "science_db_name",
        help="HBase table name",
    )

    parser.add_argument(
        "science_db_catalogs",
        help="HBase catalog name",
    )

    parser.add_argument(
        "--output-path",
        help="HDFS/local path where HFiles will be generated. "
             "Required when using bulk_load.",
    )

    args = parser.parse_args()

    # output_path is mandatory for bulk loading
    if args.type == "bulk_load" and not args.output_path:
        parser.error(
            "--output-path is required when type is 'bulk_load'"
        )

    if args.type == "classic": 
        args.output_path = None

    return args


def main():
    args = getargs()

    # Initialise Spark session
    spark = init_sparksession(
        name="science_archival_{}".format(args.type),
        shuffle_partitions=2,
    )

    # Row key
    row_key_name = "objectId_jd_candid"

    print("Processing {}".format(args.folder))

    df = load_parquet_files(args.folder)
    n_alerts = df.count()

    # Drop unused partitioning columns
    df = df.drop("year").drop("month").drop("day")

    # Drop images
    df = (
        df.drop("cutoutScience")
        .drop("cutoutTemplate")
        .drop("cutoutDifference")
    )

    # Push data to HBase
    push_full_df_to_hbase( 
        df,
        row_key_name=row_key_name,
        table_name=args.science_db_name,
        catalog_name=args.science_db_catalogs,
        bulk_loading=(args.type == "bulk_load"),
        sc=spark.sparkContext,
        output_path=args.output_path
    )

    
    print("{} alerts pushed to HBase".format(n_alerts))


if __name__ == "__main__":
    main()
