package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.udf.SpatialParquet;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class ukrainflight {

    public static void main(String[] args) throws Exception {

        String path = "../datasets/ukrainflights/ukraine_geoparquet.parquet";

        var spark = SparkConfig.createSession("../datasets/ukrainflights");

        TableSpec silver = new TableSpec("silverlayer", "ukrainflights", "");

        TableSpec silver2 = new TableSpec("silverlayer2", "ukrainflights", "");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);
        UDFRegistry udfRegistry= new SpatialParquet();
        udfRegistry.registerDecode(spark);

        testSpeedUkrainianFlightGeoParquet(spark, path);
        testSpeedUkrainianFlightBboxIndexing(spark);

        testConvertionToGeometryForSpatialLakehouse(spark,silver);
        testConvertionToGeometryForSpatialLakehouse(spark,silver2);
        testConvertionToGeometryForGeoparquet(spark,path);

    }

    public static void testSpeedUkrainianFlightGeoParquet(SparkSession spark, String path) throws Exception {

        Dataset<Row> table2 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        table2.createOrReplaceTempView("table");

        double t1 = System.currentTimeMillis();

        spark.sql("""
                    SELECT COUNT(*)
                    FROM table
                    WHERE ST_X(geom) > 25
                      AND ST_Y(geom) < 48
                """).show(false);

        System.out.println("Querying from geoparquet file time = " + (System.currentTimeMillis() - t1));

    }

    public static void testSpeedUkrainianFlightBboxIndexing(SparkSession spark) throws Exception {

        long t1 = System.currentTimeMillis();

        spark.sql("""
                SELECT
                COUNT(*) AS number
                FROM silverlayer.ukrainflights
                WHERE geometry.parts[0].coordinates[0].x >25 and
                geometry.parts[0].coordinates[0].y < 48 and
                geometry.bbox_partitioning.max_x>25 and
                geometry.bbox_partitioning.min_y<48
                """).show(false);

        long duration = System.currentTimeMillis() - t1;

        System.out.println("Querying from iceberg table time " + duration);

        System.out.println("Press ENTER to exit...");
        System.in.read();    }

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
        Dataset<Row> t1 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));


        long start = System.currentTimeMillis();

        t1.selectExpr("ST_X(geom) as x")
                .agg(expr("sum(x)"))
                .show();

        System.out.println(
                "GeoParquet decode time = "
                        + (System.currentTimeMillis() - start));

    }
}



