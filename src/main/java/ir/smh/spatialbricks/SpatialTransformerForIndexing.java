package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.udf.SparkUdfs;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import java.io.Serializable;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SpatialTransformerForIndexing implements Serializable {

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
                BucketManager.computeBucketBorders(
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

        SparkUdfs.registerFindFloorAndCeilingUdf(
                df.sparkSession(),
                broadcastBorders
        );

        return findFloorAndCeiling(df);
    }

    private static  Dataset<Row> findFloorAndCeiling(Dataset<Row> df) {

        var fromCenter = callUDF(
                "findFloorAndCeiling",
                col("geometry.center.x"),
                col("geometry.center.y")
        );

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

        var centerOk =
                col("geometry.center.x").isNotNull()
                        .and(col("geometry.center.y").isNotNull());

        var firstOk =
                size(col("geometry.parts")).gt(0)
                        .and(
                                size(
                                        col("geometry.parts")
                                                .getItem(0)
                                                .getField("coordinates")
                                ).gt(0)
                        );

        var partitionNumber = when(centerOk, fromCenter)
                .when(firstOk, fromFirst)
                .otherwise(lit(null));

        return df.withColumn(
                "geometry",
                col("geometry").withField(
                        "partition_number",
                        partitionNumber
                )
        );
    }
}
