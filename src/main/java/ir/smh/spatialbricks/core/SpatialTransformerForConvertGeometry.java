package ir.smh.spatialbricks.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

public class SpatialTransformerForConvertGeometry implements Serializable {

    static Dataset<Row> transform(
            Dataset<Row> df,
            boolean cacheResult
    ) {

        Dataset<Row> transformed = df
                .withColumn(
                        "geometry",
                        callUDF(
                                "stringOrGeomToGeometry",
                                col("geometry")
                        )
                )
                .filter(col("geometry").isNotNull());

        if (cacheResult) {
            transformed = transformed.cache();
        }

        return transformed;
    }
}
