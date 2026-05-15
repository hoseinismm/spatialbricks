package ir.smh.spatialbricks.encoder.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

public class MinInBucketUdfRegistry {

    private static List<Double> splitList;

    public static void setSplitList(List<Double> splits) {
        splitList = splits;
    }

    public static void registerAll(SparkSession spark) {

        UDF1<Long,  Long> minInBucketUDF = (value) -> {
            for (int i = 0; i < splitList.size() - 1; i++) {
                if (value >= splitList.get(i) && value < splitList.get(i + 1)) {
                    return splitList.get(i).longValue();
                }
            }
            return splitList.get(splitList.size() - 2).longValue(); // اگر مقدار برابر آخرین split باشد
        };
        spark.udf().register("minInBucketUDF", minInBucketUDF, DataTypes.LongType);


    }
}
