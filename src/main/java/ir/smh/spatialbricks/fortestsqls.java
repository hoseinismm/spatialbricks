package ir.smh.spatialbricks;


import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.createsql.IcebergTableCreator;
import ir.smh.spatialbricks.encoder.udf.ConvertToSedonaUdfRegistry;
import ir.smh.spatialbricks.createsql.IcebergTableCreatorWithPartitioning;
import ir.smh.spatialbricks.encoder.GeometryResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.sedona_sql.expressions.ST_X;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class fortestsqls {

    public static void main(String[] args) throws Exception {


        String path = "../datasets/ukrainflights/ukraine_geoparquet.parquet";

        var spark = SparkConfig.createSession("../datasets/ukrainflights");

        TableSpec silver = new TableSpec("silverlayer", "ukrainflights", "");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);

        ConvertToSedonaUdfRegistry.registerAll(spark);

        //testSpeedUkrainianFlightBboxIndexing(spark);
        //testSpeedUkrainianFlightGeoParquet(spark, path);


        testConvertionToGeometryForSpatialLakehouse(spark,silver);
        testConvertionToGeometryForGeoparquet(spark,path);

    }

    public static void testSpeedUkrainianFlightGeoParquet(SparkSession spark, String path, TableSpec silver) throws Exception {

        Dataset<Row> table2 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        IcebergTableCreator.createIcebergTableFromSchema(
                spark,
                table2.schema(),
                silver.database(),
                silver.table()
        );

        table2.createOrReplaceTempView("table");

        double t1 = System.currentTimeMillis();

        spark.sql("""
                    SELECT COUNT(*)
                    FROM table
                    WHERE ST_X(geom) > 129
                      AND ST_Y(geom) < 48
                """).show(false);

        System.out.println("Cache time = " + (System.currentTimeMillis() - t1));

    }

    public static void testSpeedUkrainianFlightBboxIndexing(SparkSession spark, TableSpec silver) throws Exception {

        String fullName = silver.database() + "." + silver.table();

        double t1 = System.currentTimeMillis();

        spark.sql("""
                SELECT
                    COUNT(*) AS number
                FROM silverlayer.ukrainflights
                WHERE geometry.parts[0].coordinates[0].x >129 and
                    geometry.parts[0].coordinates[0].y < 48   
                """).show(false);

        double duration = System.currentTimeMillis() - t1;

        System.out.println("duration for UkrainianFlight: " + duration);

        t1 = System.currentTimeMillis();

        spark.sql("""
                SELECT
                COUNT(*) AS number
                FROM silverlayer.ukrainflights
                WHERE geometry.parts[0].coordinates[0].x >129 and
                geometry.parts[0].coordinates[0].y < 50 and
                geometry.bbox_partitioning.max_x>129 and
                geometry.bbox_partitioning.min_y<50
                """).show(false);

        duration = System.currentTimeMillis() - t1;

        System.out.println("duration for UkrainianFlight with file prunning: " + duration);


        System.out.println("Press ENTER to exit...");
        System.in.read();

    }

    public static void testConvertionToGeometryForSpatialLakehouse(SparkSession spark, TableSpec silver) throws Exception {

        String fullName = silver.database() + "." + silver.table();

        Dataset<Row> t2 = spark.read()
               .format("iceberg")
              .load(fullName)
              .withColumn("geom", callUDF("decodeGeometry", col("geometry")));

        //Dataset<Row> t2 = spark.read()
        //       .format("iceberg")
        //       .load(fullName)
        //       .withColumn("geom", expr("decodeGeometry(geometry)"));

        t2.cache();
        t2.count(); // warmup

        long start = System.currentTimeMillis();

        System.out.println(t2.selectExpr("ST_X(geom)").count());

        System.out.println("Iceberg decode time = " + (System.currentTimeMillis() - start));

    }

    public static void testConvertionToGeometryForGeoparquet(SparkSession spark, String path) throws Exception {

        Dataset<Row> t1 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        t1.cache();
        t1.count();

        long start = System.currentTimeMillis();

        System.out.println(t1.selectExpr("ST_X(geom)").count());

        System.out.println("GeoParquet decode time = " + (System.currentTimeMillis() - start));

    }
}



