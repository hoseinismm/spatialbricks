package ir.smh.spatialbricks.encoder.udf;

import ir.smh.spatialbricks.core.BucketManagerForBboxIndexing;
import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Geometry;

import java.util.Map;

public interface UDFRegistry {
    DataType getGeometryType();
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
    Map<String, Object> parse(Geometry geometry);
    Row geometryToRow(Geometry geometry);
}
