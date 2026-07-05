package ir.smh.spatialbricks.core;



import ir.smh.spatialbricks.udf.UDFRegistry;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;


import static org.apache.spark.sql.functions.*;

public class SpatialTransformerForBboxIndexing implements Serializable {

    static Dataset<Row> transform(
            BucketManagerForBboxIndexing.Bucket rootBucket,
            Dataset<Row> df,
            JavaSparkContext jsc,
            UDFRegistry<?,?> udfRegistry
    )
    {
        //long n3 = df.count();
        //System.out.println("Row count n3 = " + n3);


        Broadcast<BucketManagerForBboxIndexing.Bucket> broadcastRootBuckets =
                jsc.broadcast(rootBucket);

        udfRegistry.registerBucketUdf(
                broadcastRootBuckets
        );

        return df.withColumn(
                "geometry",
                col("geometry").withField(
                        "bbox_partitioning",
                        callUDF(
                                "findBucket",
                                col("geometry")
                        )
                )
        );
    }
}
