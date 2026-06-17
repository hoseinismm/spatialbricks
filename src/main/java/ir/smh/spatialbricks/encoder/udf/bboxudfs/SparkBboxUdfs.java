package ir.smh.spatialbricks.encoder.udf.bboxudfs;

import ir.smh.spatialbricks.BucketManagerForBboxIndexing;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

interface SparkBboxUdfs {
    void registerCalculateBboxUdf(SparkSession spark);
    void registerFindBucketUdf(
            SparkSession spark,
            Broadcast<BucketManagerForBboxIndexing.Bucket> broadcastRootBuckets);

}
