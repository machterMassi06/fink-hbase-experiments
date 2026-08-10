package hbase;

import java.io.Serializable;

import org.apache.hadoop.hbase.spark.ByteArrayWrapper;
import org.apache.hadoop.hbase.spark.FamiliesQualifiersValues;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;

public class ThinBulkLoadMapper extends
    AbstractFunction1<
        Tuple2<String, Iterable<Tuple2<String, Tuple2<String,String>>>>,
        Tuple2<ByteArrayWrapper, FamiliesQualifiersValues>>
    implements Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<ByteArrayWrapper, FamiliesQualifiersValues> apply(
            Tuple2<String, Iterable<Tuple2<String, Tuple2<String,String>>>> row) {

        ByteArrayWrapper key = new ByteArrayWrapper(row._1().getBytes());

        FamiliesQualifiersValues fqv = new FamiliesQualifiersValues();

        for (Tuple2<String, Tuple2<String,String>> cell : row._2()) {

            fqv.add(
                cell._1().getBytes(),
                cell._2()._1().getBytes(),
                cell._2()._2().getBytes());
        }

        return new Tuple2<>(key, fqv);
    }
}