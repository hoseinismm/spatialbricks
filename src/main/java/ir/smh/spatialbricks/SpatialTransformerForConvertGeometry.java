package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.udf.SparkUdfs;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SpatialTransformerForConvertGeometry implements Serializable {

    static Dataset<Row> transform(
            Dataset<Row> df
    ) {

        long n1 = df.count();
        System.out.println("Row count n1 = " + n1);

        Dataset<Row> transformed = df
                .withColumn(
                        "geometry",
                        callUDF(
                                "stringOrGeomToGeometry",
                                col("geometry")
                        )
                )
                .filter(col("geometry").isNotNull());

        long n2 = transformed.count();
        System.out.println("Row count n2 = " + n2);


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
