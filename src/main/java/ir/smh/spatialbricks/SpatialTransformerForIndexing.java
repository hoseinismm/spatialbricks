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
            JavaSparkContext jsc
    ) {

        Dataset<Row> transformed = addGeohash(df);

        transformed = transformed.filter(
                col("geometry.geohash_numeric").isNotNull()
        );

        long n3 = transformed.count();
        System.out.println("Row count n3 = " + n3);

        List<Integer> borders =
                BucketManager.computeBucketBorders(
                        transformed,
                        bucketFileName
                );

        int[] bucketBorders = borders.stream()
                .mapToInt(i -> i)
                .toArray();

        Broadcast<int[]> broadcastBorders =
                jsc.broadcast(bucketBorders);

        SparkUdfs.registerFindFloorAndCeilingUdf(
                transformed.sparkSession(),
                broadcastBorders
        );

        transformed = transformed.withColumn(
                "geometry",
                col("geometry").withField(
                        "partition_number",
                        callUDF(
                                "findFloorAndCeiling",
                                col("geometry.geohash_numeric")
                        )
                )
        );

        return transformed;
    }

    private static  Dataset<Row> addGeohash(Dataset<Row> df) {

        var fromCenter = callUDF(
                "CoordinateToGeohashNumeric",
                col("geometry.center.x"),
                col("geometry.center.y")
        );

        var fromFirst = callUDF(
                "CoordinateToGeohashNumeric",
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

        var geohash = when(centerOk, fromCenter)
                .when(firstOk, fromFirst)
                .otherwise(lit(null));

        return df.withColumn(
                "geometry",
                col("geometry")
                        .withField("geohash_numeric", geohash)
        );
    }
}
