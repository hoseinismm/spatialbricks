package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.udf.ConvertToSedonaUdfRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import static org.apache.spark.sql.functions.*;

public class internatandvoicecoverage {

    public static void main(String[] args) throws Exception {


        String path = "../datasets/internet_and_voice_coverage/binaryformat/internet_and_voice_coverage/output_geoparquet/";

        var spark = SparkConfig.createSession("../datasets/internet_and_voice_coverage");

        TableSpec silver = new TableSpec("indexeddataset", "internet_and_voice_coverage", "");

        TableSpec silver2 = new TableSpec("unindexeddataset", "internet_and_voice_coverage", "");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);
        ConvertToSedonaUdfRegistry.registerAll(spark);

        testSpeedGeoParquet(spark, path);
        testSpeedBboxIndexing(spark, silver);


        testConvertionToGeometryForSpatialLakehouse(spark,silver);
        testConvertionToGeometryForSpatialLakehouse(spark,silver2);
        testConvertionToGeometryForGeoparquet(spark,path);

        System.in.read();
        spark.stop();

    }

    public static void testSpeedGeoParquet(SparkSession spark, String path) throws NoSuchTableException {

        long start = System.currentTimeMillis();

            Dataset<Row> table = spark.read()
                    .parquet(path);

            table.createOrReplaceTempView("table");

            spark.sql("""
                SELECT COUNT (*) FROM table
                 WHERE
                  ST_XMin(ST_GeomFromWKB(geometry))>-161   and
                  ST_XMax(ST_GeomFromWKB(geometry))<-159   and
                  ST_YMin(ST_GeomFromWKB(geometry))>59     and
                  ST_YMax(ST_GeomFromWKB(geometry))<61
            """).show(false);


        System.out.println("Querying from geoparquet file time = " + (System.currentTimeMillis() - start));
    }


    public static void testSpeedBboxIndexing(SparkSession spark, TableSpec silver) throws Exception {

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();

        Dataset<Row> t1 = spark.read()
                .format("iceberg")
                .load(fullName);

        t1.createOrReplaceTempView("table");

        String sql = String.format("""
                   SELECT COUNT(*)
                   FROM %s
                   WHERE
                      ST_XMin(decodeGeometry(geometry))>-161   and
                      ST_XMax(decodeGeometry(geometry))<-159   and
                      ST_YMin(decodeGeometry(geometry))>59     and
                      ST_YMax(decodeGeometry(geometry))<61
                   AND
                      (
                      geometry.bbox_partitioning.min_x < -159 AND
                      geometry.bbox_partitioning.max_x > -161 AND
                      geometry.bbox_partitioning.min_y < 61 AND
                      geometry.bbox_partitioning.max_y > 59
                      )
                
                   """, fullName);
        spark.sql(sql).show(false);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Querying from iceberg table time " + duration);

    }

    public static void testConvertionToGeometryForSpatialLakehouse(SparkSession spark, TableSpec silver) throws Exception {

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();
        String sql = String.format("""
                SELECT SUM(ST_Area(decodeGeometry(geometry)))
                FROM %s
                """, fullName);
        spark.sql(sql).show(false);

        System.out.println("Iceberg"+fullName+" decode time = " + (System.currentTimeMillis() - start));

    }

    public static void testConvertionToGeometryForGeoparquet(SparkSession spark, String path) throws Exception {
        long start = System.currentTimeMillis();
        Dataset<Row> t1 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        t1.selectExpr("ST_Area(geom) as x")
                .agg(expr("sum(x)"))
                .show();

        System.out.println(
                "GeoParquet decode time = "
                        + (System.currentTimeMillis() - start));

    }
}

