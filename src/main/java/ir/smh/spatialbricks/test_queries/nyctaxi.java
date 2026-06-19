package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.encoder.udf.SpatialParquet;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class nyctaxi {

    public static void main(String[] args) throws Exception {

        String path = "../datasets/nyc_taxi/yellow_tripdata_2009_q1_geoparquet.parquet";

        var spark = SparkConfig.createSession("../datasets/nyc_taxi");

        TableSpec silver = new TableSpec("silverlayer", "nyc_taxi", "");

        TableSpec silver2 = new TableSpec("silverlayer2", "nyc_taxi", "");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);
        UDFRegistry udfRegistry= new SpatialParquet();
        udfRegistry.registerDecode(spark);

        testSpeedGeoParquet(spark, path);
        testSpeedBboxIndexing(spark);

        testConvertionToGeometryForSpatialLakehouse(spark,silver);
        testConvertionToGeometryForSpatialLakehouse(spark,silver2);
        testConvertionToGeometryForGeoparquet(spark,path);
    }

    public static void testSpeedGeoParquet(SparkSession spark, String path) throws Exception {

        Dataset<Row> table2 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        table2.createOrReplaceTempView("table");

        double t1 = System.currentTimeMillis();

        spark.sql("""
                    SELECT COUNT(*)
                    FROM table
                    WHERE
                    (ST_X(geom) < -74.02 OR ST_X(geom) > -73.9)
                    OR
                    (ST_Y(geom) < 40.7 OR ST_Y(geom) > 40.88)
                """).show(false);

        System.out.println("Querying from geoparquet file time = " + (System.currentTimeMillis() - t1));
    }
    public static void testSpeedBboxIndexing(SparkSession spark) throws Exception {

        long t1 = System.currentTimeMillis();
        spark.sql("""
                SELECT
                    COUNT(*) AS number
                FROM silverlayer.nyc_taxi
                WHERE
                      (
                      geometry.parts[0].coordinates[0].x < -74.02 OR
                      geometry.parts[0].coordinates[0].x > -73.90 OR
                      geometry.parts[0].coordinates[0].y < 40.70 OR
                      geometry.parts[0].coordinates[0].y > 40.88
                      )
                AND Not
                      (
                      geometry.bbox_partitioning.max_x < -73.90 AND
                      geometry.bbox_partitioning.min_x > -74.02 AND
                      geometry.bbox_partitioning.max_y < 40.88 AND
                      geometry.bbox_partitioning.min_y > 40.70
                      )
                """).show(false);

        long duration = System.currentTimeMillis() - t1;

        System.out.println("Querying from iceberg table time " + duration);

        System.out.println("Press ENTER to exit...");
        System.in.read();

    }

    public static void testConvertionToGeometryForSpatialLakehouse(SparkSession spark, TableSpec silver) throws Exception {

        String fullName = silver.database() + "." + silver.table();

        Dataset<Row> t1 = spark.read()
             .format("iceberg")
             .load(fullName)
             .withColumn("geom", callUDF("decodeGeometry", col("geometry")));


        long start = System.currentTimeMillis();

        t1.selectExpr("ST_X(geom) as x")
                .agg(expr("sum(x)"))
                .show();

        System.out.println("Iceberg"+fullName+" decode time = " + (System.currentTimeMillis() - start));

    }

    public static void testConvertionToGeometryForGeoparquet(SparkSession spark, String path) throws Exception {
        Dataset<Row> t2 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        long start = System.currentTimeMillis();

        t2.selectExpr("ST_X(geom) as x")
                .agg(expr("sum(x)"))
                .show();

        System.out.println(
                "GeoParquet decode time = "
                        + (System.currentTimeMillis() - start));

    }
}



/*
  SELECT
                    COUNT(*) AS number
                FROM silverlayer.nyc_taxi
                WHERE
                (

                    (
                    geometry.parts[0].coordinates[0].x < -74.02 OR
                    geometry.parts[0].coordinates[0].x > -73.90 OR
                    geometry.parts[0].coordinates[0].y < 40.70 OR
                    geometry.parts[0].coordinates[0].y > 40.88
                    )
                   AND Not
                    (
                    geometry.bbox_partitioning.max_x < -73.90 AND
                    geometry.bbox_partitioning.min_x > -74.02 AND
                    geometry.bbox_partitioning.max_y < 40.88 AND
                    geometry.bbox_partitioning.min_y > 40.70
                    )
                )
                OR
                (
                    (
                    geometry.bbox_partitioning.max_x < -74.02
                    OR
                    geometry.bbox_partitioning.min_x > -73.90
                    )
                    AND
                    (
                    geometry.bbox_partitioning.max_y < 40.70
                    OR
                    geometry.bbox_partitioning.min_y > 40.88
                    )
                )
 */



