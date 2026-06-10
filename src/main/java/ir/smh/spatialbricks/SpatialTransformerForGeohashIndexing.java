package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.udf.SparkGeohashUdfs;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import java.io.Serializable;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SpatialTransformerForGeohashIndexing implements Serializable {

    static Dataset<Row> transform(
            Dataset<Row> df,
            String bucketFileName,
            JavaSparkContext jsc,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize,
            Long totalRowsHint
    )
    {
        long n3 = df.count();
        System.out.println("Row count n3 = " + n3);

        List<Integer> borders =
                BucketManagerForGeohashIndexing.computeBucketBorders(
                        df,
                        bucketFileName,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        totalRowsHint
                );

        int[] bucketBorders = borders.stream()
                .mapToInt(i -> i)
                .toArray();

        Broadcast<int[]> broadcastBorders =
                jsc.broadcast(bucketBorders);

        SparkGeohashUdfs.registerFindFloorAndCeilingUdf(
                df.sparkSession(),
                broadcastBorders
        );

        return findFloorAndCeiling(df);
    }

    private static  Dataset<Row> findFloorAndCeiling(Dataset<Row> df) {

        var fromFirst = callUDF(
                "findFloorAndCeiling",
                col("geometry.parts")
                        .getItem(0)
                        .getField("coordinates")
                        .getItem(0)
                        .getField("x"),
                col("geometry.parts")
                        .getItem(0)
                        .getField("coordinates")
                        .getItem(0)
                        .getField("y")
        );

        var firstOk =
                size(col("geometry.parts")).gt(0)
                        .and(
                                size(
                                        col("geometry.parts")
                                                .getItem(0)
                                                .getField("coordinates")
                                ).gt(0)
                        );

        var partitionNumber = when(firstOk, fromFirst)
                .otherwise(lit(null));

        return df.withColumn(
                "geometry",
                col("geometry").withField(
                        "geohash_partitioning",
                        partitionNumber
                )
        );
    }
}
