package ir.smh.spatialbricks.encoder.udf;

import ir.smh.spatialbricks.BucketManagerForBboxIndexing;
import ir.smh.spatialbricks.encoder.GeometryReader;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface UDFRegistry {
    void registerGeometryUdf(SparkSession spark, GeometryReader adapter);
    void registerBucketUdf(SparkSession spark,
                                  Broadcast<BucketManagerForBboxIndexing.Bucket> broadcast);
    void registerBboxUdf(SparkSession spark);
    void registerDecode(SparkSession spark);
    Dataset<Row> addPointGeometryColumn(
            Dataset<Row> df,
            String xColumn,
            String yColumn,
            String geometryColumnName
    );
}
