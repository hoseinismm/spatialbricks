package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfigLocal;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.udf.SpatialParquet;
import ir.smh.spatialbricks.udf.UDFRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import static org.apache.spark.sql.functions.*;

public class aubuildings {

    public static void main(String[] args) throws Exception {

        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            final int runs = 10;

            SparkSession spark = createSpark();

            try {

                String path = "../datasets/aubuildings/bronze/aubuildings/output_geoparquet/*.parquet";

                TableSpec silverUnindexed = new TableSpec("silverUnindexed", "aubuildings", "");
                TableSpec silverIndexed = new TableSpec("silverIndexed", "aubuildings", "");
                TableSpec flattenSilverUnindexed = new TableSpec("flattenSilverUnindexed", "aubuildings", "");
                TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "aubuildings", "");

                long[][] results = runBenchmarks(
                        spark,
                        runs,
                        path,
                        silverUnindexed,
                        silverIndexed,
                        flattenSilverUnindexed,
                        flattenSilverIndexed
                );

            writeResults(results, runs);

            }   finally {
                spark.stop();
            }

        } finally {
            PowerPlanUtil.setPowerPlan(PowerPlanUtil.BALANCED);
        }
    }

    private static SparkSession createSpark() {

        SparkSession spark =
                SparkConfigLocal.createSession("../datasets/aubuildings");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);

        return spark;
    }

    private static long[][] runBenchmarks(
            SparkSession spark,
            int runs,
            String path,
            TableSpec silverUnindexed,
            TableSpec silverIndexed,
            TableSpec flattenSilverUnindexed,
            TableSpec flattenSilverIndexed
            ) throws Exception {

        long[][] results = new long[10][runs];

        for (int i = 0; i < runs; i++) {

            System.out.println("Run " + (i + 1));

            results[0][i] = testQueryInGeoParquet(spark, path);
            results[1][i] = testQuery(spark, silverUnindexed,false,new SpatialParquet());
            results[2][i] = testQuery(spark, silverIndexed, true, new SpatialParquet());
            results[3][i] = testQuery(spark, flattenSilverUnindexed,false, new FlattenSpatialParquet() );
            results[4][i] = testQuery(spark, flattenSilverIndexed, true, new FlattenSpatialParquet() );
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

        try (PrintWriter out = new PrintWriter("benchmark_for_aubuildings")) {

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
                   SELECT
                         SUM(ST_AreaSpheroid(geom)) AS total_area
                     FROM table
                     WHERE ST_Contains(
                              ST_PolygonFromEnvelope(
                                  115.74,
                                  -32.18,
                                  116.05,
                                  -31.75
                              ),
                              geom
                     );
                """).show(false);
        long duration=System.currentTimeMillis()-t1;

        System.out.println("Querying from geoparquet file time = " + duration);

        return duration;
    }
    private static long testQuery(

            SparkSession spark,
            TableSpec table,
            boolean indexed, UDFRegistry udfRegistry) throws IOException {

        udfRegistry.registerDecode(spark);


        String bboxFilter = indexed
                ? """
                 WHERE geometry.bbox_partitioning.min_x < 116.05
                       AND geometry.bbox_partitioning.min_y < -31.75
                       AND geometry.bbox_partitioning.max_x > 115.74
                       AND geometry.bbox_partitioning.max_y > -32.18
              """
                : "";

        String sql = """
        SELECT
        SUM(ST_AreaSpheroid(geom)) AS total_area
                FROM (
                    SELECT
                        decodeGeometry(geometry) AS geom
                     FROM  %s
        %s
                ) t
                WHERE ST_Contains(
                    ST_PolygonFromEnvelope(
                        115.74,
                        -32.18,
                        116.05,
                        -31.75
                    ),
                    geom
                );
        """.formatted(
                table.database() + "." + table.table(),
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
                        "ST_AreaSpheroid(geom) as area"
                )
                .agg(expr("avg(area)"))
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
                        "ST_AreaSpheroid(geom) as area"
                )
                .agg(expr("avg(area)"))
                .show();

        long duration = System.currentTimeMillis() - start;

        System.out.println(
                fullName+ " decode time = "
                        + duration);

        return duration;
    }
}


