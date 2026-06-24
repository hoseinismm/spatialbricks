package ir.smh.spatialbricks.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

public class SpatialTransformerForConvertGeometry implements Serializable {

    static Dataset<Row> transform(
            Dataset<Row> df
    ) {

        //long n1 = df.count();
        //System.out.println("Row count n1 = " + n1);

        Dataset<Row> transformed = df
                .withColumn(
                        "geometry",
                        callUDF(
                                "stringOrGeomToGeometry",
                                col("geometry")
                        )
                )
                .filter(col("geometry").isNotNull());

        //long n2 = transformed.count();
        //System.out.println("Row count n2 = " + n2);

        return transformed;
    }

}
