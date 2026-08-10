package hbase;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.spark.ByteArrayWrapper;
import org.apache.hadoop.hbase.spark.FamiliesQualifiersValues;
import org.apache.hadoop.hbase.spark.FamilyHFileWriteOptions;
import org.apache.hadoop.hbase.spark.HBaseContext;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import scala.Function1;
import scala.Tuple2;



public class ThinBulkLoadWrapper {


    public static void bulkLoadThinRows(

            HBaseContext context,

            Dataset<Row> df,

            Map<String, Map<String,String>> columns,

            String rowkeyColumn,

            String tableName,

            String stagingDir,

            Map<byte[], FamilyHFileWriteOptions> options,

            boolean compactionExclude,

            long maxSize

    ) {



        JavaRDD<
            Tuple2<
                String,
                Iterable<
                    Tuple2<
                        String,
                        Tuple2<String,String>
                    >
                >
            >
        > rdd =


        df.javaRDD().map(row -> {


            String rowkey =
                row.getAs(rowkeyColumn).toString();



            List<
                Tuple2<
                    String,
                    Tuple2<String,String>
                >
            > cells = new ArrayList<>();



            for (Map.Entry<String, Map<String,String>> entry
                    : columns.entrySet()) {



                String columnName =
                    entry.getKey();



                if (columnName.equals(rowkeyColumn)) {
                    continue;
                }



                Map<String,String> meta =
                    entry.getValue();



                String cf =
                    meta.get("cf");



                String qualifier =
                    meta.get("col");



                Object valueObject =
                    row.getAs(columnName);



                if (valueObject == null) {
                    continue;
                }



                String value =
                    valueObject.toString();



                cells.add(

                    new Tuple2<>(

                        cf,

                        new Tuple2<>(

                            qualifier,

                            value

                        )

                    )

                );

            }



            return new Tuple2<>(

                rowkey,

                cells

            );


        });



        Function1<

            Tuple2<
                String,
                Iterable<
                    Tuple2<
                        String,
                        Tuple2<String,String>
                    >
                >
            >,

            Tuple2<
                ByteArrayWrapper,
                FamiliesQualifiersValues
            >

        > mapper =

            new ThinBulkLoadMapper();



        context.bulkLoadThinRows(

            rdd.rdd(),

            TableName.valueOf(tableName),

            mapper,

            stagingDir,

            options,

            compactionExclude,

            maxSize

        );

    }

}