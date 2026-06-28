package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.encoder.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.encoder.udf.SpatialParquet;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import static org.apache.spark.sql.functions.*;

public class internatandvoicecoverage {

    public static void main(String[] args) throws Exception {

        SparkSession spark =  createSpark();

        try {

            PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

            try {

                final int runs = 10;


                String path = "../datasets/internet_and_voice_coverage/bronze/internet_and_voice_coverage/output_geoparquet/";

                TableSpec silverUnindexed = new TableSpec("silverUnindexed", "internet_and_voice_coverage", "");
                TableSpec silverIndexed = new TableSpec("silverIndexed", "internet_and_voice_coverage", "");
                TableSpec flattenSilverUnindexed = new TableSpec("flattenSilverUnindexed", "internet_and_voice_coverage", "");
                TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "internet_and_voice_coverage", "");

                long[][] results = runBenchmarks(
                        runs,
                        spark,
                        path,
                        silverUnindexed,
                        silverIndexed,
                        flattenSilverUnindexed,
                        flattenSilverIndexed
                );

                writeResults(results, runs);

            } finally {
                PowerPlanUtil.setPowerPlan(PowerPlanUtil.BALANCED);
            }

        } finally {
            spark.stop();
        }

    }

    private static SparkSession createSpark() {

        SparkSession spark =
                SparkConfigLocal.createSession("../datasets/internet_and_voice_coverage");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);

        return spark;
    }



    private static long[][] runBenchmarks(
            int runs,
            SparkSession spark,
            String path,
            TableSpec silverUnindexed,
            TableSpec silverIndexed,
            TableSpec flattenSilverUnindexed,
            TableSpec flattenSilverIndexed
            ) throws Exception {

        long[][] results = new long[10][runs];

        for (int i = 0; i < runs; i++) {

                System.out.println("Run " + (i + 1));

                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[0][i] = testQueryInGeoParquet(spark, path);
                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[1][i] = 0; //testQueryInUnindexed(spark, silverUnindexed, new SpatialParquet());
                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[2][i] = testQueryInIndexed(spark, silverIndexed, new SpatialParquet());
                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[3][i] = testQueryInUnindexed(spark, flattenSilverUnindexed, new FlattenSpatialParquet());
                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[4][i] = testQueryInIndexed(spark, flattenSilverIndexed, new FlattenSpatialParquet());
                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[5][i] = testDecodeForGeoparquet(spark, path);
                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[6][i] =0;// testDecode(spark, silverUnindexed, new SpatialParquet());
                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[7][i] = testDecode(spark, silverIndexed, new SpatialParquet());
                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[8][i] = testDecode(spark, flattenSilverUnindexed, new FlattenSpatialParquet());
                spark.catalog().clearCache();
                System.gc();
                Thread.sleep(3000);
                results[9][i] = testDecode(spark, flattenSilverIndexed, new FlattenSpatialParquet());

        }

        return results;
    }

    private static void writeResults(long[][] results, int runs)
            throws FileNotFoundException {

        String[] names = {
                "GeoParquet",
                "Spatial Unindexed",
                "Spatial Indexed",
                "Flatten Unindexed",
                "Flatten Indexed",
                "GeoParquet",
                "Spatial Unindexed",
                "Spatial Indexed",
                "Flatten Unindexed",
                "Flatten Indexed"

        };

        try (PrintWriter out = new PrintWriter("benchmark_internet_and_voice_coverage2.csv")) {

            out.print("Test");

            for (int i = 1; i <= runs; i++) {
                out.print(",Run" + i);
            }

            out.println();

            for (int t = 0; t < names.length; t++) {

                out.print(names[t]);

                for (int r = 0; r < runs; r++) {
                    out.print("," + results[t][r]);
                }

                out.println();
            }
        }
    }

    public static long testQueryInGeoParquet(SparkSession spark, String path) throws NoSuchTableException {

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

            long duration= System.currentTimeMillis() - start;


        System.out.println("Querying from geoparquet file time = " + duration);
        spark.catalog().dropTempView("table");
        table = null;

        return duration;

    }


    public static long testQueryInIndexed(SparkSession spark, TableSpec silver, UDFRegistry udfRegistry) throws Exception {

        udfRegistry.registerDecode(spark);

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

        System.out.println("Querying from  " +fullName+ " = " + duration);

        t1 = null;
        spark.catalog().dropTempView("table");


        return  duration;
    }

    public static long testQueryInUnindexed(SparkSession spark, TableSpec silver, UDFRegistry udfRegistry) throws Exception {

        udfRegistry.registerDecode(spark);

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
                
                   """, fullName);
        spark.sql(sql).show(false);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Querying from  " +fullName+ " = " + duration);

        t1 = null;
        spark.catalog().dropTempView("table");

        return  duration;
    }

    public static long testDecode(SparkSession spark, TableSpec silver, UDFRegistry udfRegistry) throws Exception {

        udfRegistry.registerDecode(spark);

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();
        String sql = String.format("""
                SELECT SUM(ST_Area(decodeGeometry(geometry)))
                FROM %s
                """, fullName);
        spark.sql(sql).show(false);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Iceberg"+fullName+" decode time = " + duration);

        return  duration;
    }

    public static long testDecodeForGeoparquet(SparkSession spark, String path) throws Exception {
        long start = System.currentTimeMillis();
        Dataset<Row> t1 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        t1.selectExpr("ST_Area(geom) as x")
                .agg(expr("sum(x)"))
                .show();

        long duration = System.currentTimeMillis() - start;

        System.out.println(
                "GeoParquet decode time = "
                        + duration);
        t1 = null;


        return  duration;
    }
}

