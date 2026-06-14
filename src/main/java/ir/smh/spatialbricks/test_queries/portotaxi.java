package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.createsql.IcebergTableCreator;
import ir.smh.spatialbricks.encoder.udf.ConvertToSedonaUdfRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class portotaxi {

    public static void main(String[] args) throws Exception {

        String path = "../datasets/portotaxi/porto_taxi_chunk_*.parquet";

        var spark = SparkConfig.createSession("../datasets/portotaxi");

        TableSpec silver = new TableSpec("silverlayer", "portotaxi", "");

        TableSpec silver2 = new TableSpec("silverlayer2", "portotaxi", "");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);
        ConvertToSedonaUdfRegistry.registerAll(spark);

        testSpeedGeoParquet(spark, path);
        testSpeedBboxIndexing(spark, silver);

        //testConvertionToGeometryForSpatialLakehouse(spark,silver);
        //testConvertionToGeometryForSpatialLakehouse(spark,silver2);
        //testConvertionToGeometryForGeoparquet(spark,path);
    }

    public static void testSpeedGeoParquet(SparkSession spark, String path) throws NoSuchTableException {



            String table = "a.b";

            Dataset<Row> t1 = spark.read()
                    .parquet(path)
                    .withColumn("geom", expr("ST_GeomFromWKB(geometry)"))
                    .filter(expr("ST_NumGeometries(geom) >= 2"))
                    .withColumn("geom", expr("ST_AsBinary(geom)"))
                    .select("geom");

            IcebergTableCreator.createIcebergTableFromSchema(
                    spark,  t1.schema(), "a", "b"
            );
            System.out.println("table is created");

            t1.writeTo(table).append();
            System.out.println("data appended");

        long start = System.currentTimeMillis();

            Dataset<Row> t2 = spark.read()
                    .format("iceberg")
                    .load(table)
                    .withColumn("geom", expr("ST_GeomFromWKB(geom)"));

            t2.createOrReplaceTempView("table");

            spark.sql("""
            SELECT COUNT(*) AS number
            FROM table
            WHERE
       
                ST_X(
                    ST_PointN(ST_LineFromMultiPoint(geom), 1)
                ) BETWEEN -8.62 AND -8.56

            AND
                ST_Y(
                    ST_PointN(ST_LineFromMultiPoint(geom), 1)
                ) BETWEEN 41.15 AND 41.19
        """).show(false);

        System.out.println("Querying from geoparquet file time = " + (System.currentTimeMillis() - start));
    }
    public static void testSpeedBboxIndexing(SparkSession spark, TableSpec silver) throws Exception {

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();

        Dataset<Row> t1 = spark.read()
                .format("iceberg")
                .load(fullName)
                .filter(expr("size(geometry.parts) >= 2"))
                .withColumn("geom", callUDF("decodeGeometry", col("geometry")))
                .withColumn("geom",  expr("ST_LineFromMultiPoint(geom)"));

        t1.createOrReplaceTempView("table");

        spark.sql("""
                SELECT
                    COUNT(*) AS number
                FROM table
                WHERE
                      (
                      geometry.parts[0].coordinates[0].x BETWEEN -8.62 AND -8.56
                      AND
                      geometry.parts[0].coordinates[0].y BETWEEN 41.15 AND 41.19
                      )
                AND
                      (
                      geometry.bbox_partitioning.min_x < -8.56 AND
                      geometry.bbox_partitioning.max_x > -8.62 AND
                      geometry.bbox_partitioning.min_y < 41.19 AND
                      geometry.bbox_partitioning.max_y > 41.15
                      )
                      
                """).show(false);

        long duration = System.currentTimeMillis() - start;

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
        Dataset<Row> t2 = spark.read()
                .format("iceberg")
                .load(fullName).withColumn(
                        "geom",
                        expr("ST_Point(geometry.parts[0].coordinates[0].x, geometry.parts[0].coordinates[0].y)")
                );

        long start = System.currentTimeMillis();

        t2.selectExpr("ST_X(geom) as x")
                .agg(expr("sum(x)"))
                .show();

        System.out.println("Iceberg"+fullName+" decode time = " + (System.currentTimeMillis() - start));

    }

    public static void testConvertionToGeometryForGeoparquet(SparkSession spark, String path) throws Exception {
        Dataset<Row> t1 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));
        Dataset<Row> bigger = t1;


        long start = System.currentTimeMillis();

        bigger.selectExpr("ST_X(geom) as x")
                .agg(expr("sum(x)"))
                .show();

        System.out.println(
                "GeoParquet decode time = "
                        + (System.currentTimeMillis() - start));

    }
}

//
//SELECT
//COUNT(*) AS number
//FROM table
//WHERE
//        (
//                geometry.parts[0].coordinates[0].x BETWEEN -8.62 AND -8.56
//                AND
//                geometry.parts[0].coordinates[0].y BETWEEN 41.15 AND 41.19
//        )

//And
//ST_Length(geom) > 0.1
//  اگر file pruning هم نباشد چون از ارایه میخواند نه توابع سدونا سریعتر است.



