package ir.smh.spatialbricks.core;



import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;


import static org.apache.spark.sql.functions.*;

public class SpatialTransformerForBboxIndexing implements Serializable {

    static Dataset<Row> transform(
            Dataset<Row> df,
            String bucketFileName,
            JavaSparkContext jsc,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize,
            Long totalRowsHint,
            UDFRegistry udfregistry
    )
    {
        //long n3 = df.count();
        //System.out.println("Row count n3 = " + n3);

        BucketManagerForBboxIndexing.Bucket rootBucket =
                BucketManagerForBboxIndexing.computeBucketBorders(
                        df,
                        bucketFileName,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        totalRowsHint, udfregistry
                );

        Broadcast<BucketManagerForBboxIndexing.Bucket> broadcastRootBuckets =
                jsc.broadcast(rootBucket);

        udfregistry.registerBucketUdf(
                df.sparkSession(),
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
