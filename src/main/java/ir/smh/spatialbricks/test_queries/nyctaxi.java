package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.encoder.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.encoder.udf.SpatialParquet;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import static org.apache.spark.sql.functions.*;

public class nyctaxi {

    public static void main(String[] args) throws Exception {

        final int runs = 10;

        SparkSession spark = createSpark();

        String path = "../datasets/nyc_taxi/yellow_tripdata_2009-0*_geoparquet_*.parquet";

        TableSpec silverIndexed = new TableSpec("silverIndexed", "nyc_taxi", "");
        TableSpec silverUnindexed = new TableSpec("silverUnindexed", "nyc_taxi", "");
        TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "nyc_taxi", "");
        TableSpec flattenSilverUnindexed = new TableSpec("FlattenSilverUnindexed", "nyc_taxi", "");



        long[][] results = runBenchmarks(
                spark,
                runs,
                path,
                silverIndexed,
                silverUnindexed,
                flattenSilverIndexed,
                flattenSilverUnindexed
        );

        writeResults(results, runs);

        spark.stop();
    }

    private static SparkSession createSpark() {

        SparkSession spark =
                SparkConfigLocal.createSession("../datasets/nyc_taxi");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);

        return spark;
    }

    private static long[][] runBenchmarks(
            SparkSession spark,
            int runs,
            String path,
            TableSpec silverIndexed,
            TableSpec silverUnindexed,
            TableSpec flattenSilverIndexed,
            TableSpec flattenSilverUnindexed) throws Exception {

        long[][] results = new long[10][runs];

        for (int i = 0; i < runs; i++) {

            System.out.println("Run " + (i + 1));

            results[0][i] = testQueryInGeoParquet(spark, path);
            results[1][i] = testQuery(spark, silverUnindexed,false,false);
            results[2][i] = testQuery(spark, silverIndexed, false, true);
            results[3][i] = testQuery(spark, flattenSilverUnindexed,true, false );
            results[4][i] = testQuery(spark, flattenSilverIndexed, true, true);
            results[5][i] = testDecodeForGeoparquet(spark, path);
            results[6][i] = testDecode(spark, silverUnindexed, new SpatialParquet());
            results[7][i] = testDecode(spark, silverIndexed, new SpatialParquet());
            results[8][i] = testDecode(spark, flattenSilverUnindexed, new FlattenSpatialParquet());
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

        try (PrintWriter out = new PrintWriter("benchmark_for_nyc_taxi.csv")) {

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


    public static long testQueryInGeoParquet(SparkSession spark, String path) throws Exception {

        Dataset<Row> table2 = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        table2.createOrReplaceTempView("table");

        long t1 = System.currentTimeMillis();

        spark.sql("""
                    SELECT COUNT(*)
                    FROM table
                    WHERE
                    (ST_X(geom) < -74.02 OR ST_X(geom) > -73.9)
                    OR
                    (ST_Y(geom) < 40.7 OR ST_Y(geom) > 40.88)
                """).show(false);
        long duration=System.currentTimeMillis()-t1;

        System.out.println("Querying from geoparquet file time = " + duration);

        return duration;
    }
    private static long testQuery(
            SparkSession spark,
            TableSpec table,
            boolean flatten,
            boolean indexed) throws IOException {

        String xExpr = flatten
                ? "geometry.x[0]"
                : "geometry.parts[0].coordinates[0].x";

        String yExpr = flatten
                ? "geometry.y[0]"
                : "geometry.parts[0].coordinates[0].y";

        String bboxFilter = indexed
                ? """
              AND NOT (
                  geometry.bbox_partitioning.max_x < -73.90 AND
                  geometry.bbox_partitioning.min_x > -74.02 AND
                  geometry.bbox_partitioning.max_y < 40.88 AND
                  geometry.bbox_partitioning.min_y > 40.70
              )
              """
                : "";

        String sql = """
        SELECT COUNT(*) AS number
        FROM %s
        WHERE
        (
            %s < -74.02 OR
            %s > -73.90 OR
            %s < 40.70 OR
            %s > 40.88
        )
        %s
        """.formatted(
                table.database() + "." + table.table(),
                xExpr,
                xExpr,
                yExpr,
                yExpr,
                bboxFilter
        );

        long t1 = System.currentTimeMillis();

        spark.sql(sql).show(false);

        long duration = System.currentTimeMillis() - t1;

        System.out.println("Querying from iceberg table time " + duration);

        return duration;
    }


    public static long testDecodeForGeoparquet(SparkSession spark, String path) throws Exception {

        Dataset<Row> t = spark.read()
             .parquet(path)
             .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        long start = System.currentTimeMillis();

        t.selectExpr(
                        "ST_X(geom) - ST_Y(geom) as diff"
                )
                .agg(expr("sum(diff)"))
                .show();
        long duration = System.currentTimeMillis() - start;

        System.out.println("geoparquet decode time = " + duration);

        return duration;

    }

    public static long testDecode(SparkSession spark, TableSpec table, UDFRegistry udfregistry) throws Exception {

        String fullName= table.database() + "." + table.table();

        udfregistry.registerDecode(spark);

        Dataset<Row> t = spark.read()
                .format("iceberg")
                .load(fullName).withColumn(
                        "geom",
                        expr("decodeGeometry(geometry)")
                );

        long start = System.currentTimeMillis();

        t.selectExpr(
                        "ST_X(geom) - ST_Y(geom) as diff"
                )
                .agg(expr("sum(diff)"))
                .show();

        long duration = System.currentTimeMillis() - start;

        System.out.println(
                fullName+ " decode time = "
                        + duration);

        return duration;
    }
}


