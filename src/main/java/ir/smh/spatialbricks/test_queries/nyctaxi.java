package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.udf.WKBIndexedParquet;
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

public class nyctaxi {


    private  static final SparkSession spark =   SparkConfigLocal.createSession("../datasets/nyc_taxi");

    private static final UDFRegistry<?, ?> wkbRegistry =
            new WKBIndexedParquet(spark);

    private static final UDFRegistry<?, ?> spatialRegistry =
            new SpatialParquet(spark);

    private static final UDFRegistry<?, ?> flattenRegistry =
            new FlattenSpatialParquet(spark);


    private static final TableSpec wkbUnindexed =
            new TableSpec("wkbUnindexed", "nyc_taxi", "");

    private static final TableSpec wkbIndexed =
            new TableSpec("wkbIndexed", "nyc_taxi", "");

    private static final TableSpec silverUnindexed =
            new TableSpec("silverUnindexed", "nyc_taxi", "");

    private static final TableSpec silverIndexed =
            new TableSpec("silverIndexed", "nyc_taxi", "");

    private static final TableSpec flattenSilverUnindexed =
            new TableSpec("flattenSilverUnindexed", "nyc_taxi", "");

    private static final TableSpec flattenSilverIndexed =
            new TableSpec("flattenSilverIndexed", "nyc_taxi", "");

    private static final TableSpec flattenSilverIndexedIncremental =
            new TableSpec("flattenSilverIndexedIncremental", "nyc_taxi", "");

    public static void main(String[] args) throws Exception {

        try {

            int runs = 10;

            PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

            SedonaContext.create(spark);
            SedonaSQLRegistrator.registerAll(spark);

            try {
                long[][] results = runBenchmarks(runs);

                writeResults(results, runs);

            } finally {

                spark.stop();
            }

        } finally {
            PowerPlanUtil.setPowerPlan(PowerPlanUtil.BALANCED);
        }
    }

    private static long[][] runBenchmarks( int runs) throws IOException {

        long[][] results = new long[12][runs];

        for (int i = 0; i < runs; i++) {

            System.out.println("Run " + (i + 1));

            results[0][i] = testQuery2(wkbUnindexed, GeometryFormat.WKB,false);
            results[1][i] = testQuery2(wkbIndexed, GeometryFormat.WKB,true);
            results[2][i] =0;// testQuery2(silverUnindexed,GeometryFormat.SPATIAL,false);
            results[3][i] =0;// testQuery2(silverIndexed,GeometryFormat.SPATIAL,true);
            results[4][i] =0;// testQuery2(flattenSilverUnindexed,GeometryFormat.FLATTEN,false);
            results[5][i] =0;// testQuery2(flattenSilverIndexed,GeometryFormat.FLATTEN,true);
            results[6][i] =0;// testQuery(flattenSilverIndexedIncremental,GeometryFormat.FLATTEN,true);
            results[7][i] =0;// testDecode2(silverUnindexed);
            results[8][i] =0;// testDecode3(flattenSilverUnindexed);
            results[9][i] =0;// testDecode(wkbUnindexed, wkbRegistry);
            results[10][i] =0;// testDecode(silverUnindexed, spatialRegistry);
            results[11][i] =0;// testDecode(flattenSilverUnindexed, flattenRegistry);
        }

        return results;
    }

    private static void writeResults(long[][] results, int runs)
            throws FileNotFoundException {

        String[] names = {

                "WKB Unindexed",
                "WKB Indexed",
                "Spatial Unindexed",
                "Spatial Indexed",
                "Flatten Unindexed",
                "Flatten Indexed",
                "Flatten Indexed Incremental",
                "Spatial Decoded",
                "Flatten Decoded",
                "WKB Unindexed",
                "Spatial Unindexed",
                "Flatten Unindexed"

        };

        try (PrintWriter out = new PrintWriter("benchmark34_for_nyc_taxi6.csv")) {

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

    enum GeometryFormat {
        WKB,
        SPATIAL,
        FLATTEN
    }


    private static long testQuery( TableSpec table, GeometryFormat format, boolean indexed) throws IOException {

        switch (format) {
            case WKB -> wkbRegistry.registerDecode();
            case SPATIAL -> spatialRegistry.registerDecode();
            case FLATTEN -> flattenRegistry.registerDecode();
        }

        String fromClause;
        String xExpr;
        String yExpr;

        switch (format) {
            case WKB -> {
                fromClause = """
                    (
                        SELECT
                            decodeGeometry(geometry) AS geom,
                            geometry
                        FROM %s
                    ) t
                    """.formatted(table.database() + "." + table.table());

                xExpr = "ST_X(geom)";
                yExpr = "ST_Y(geom)";
            }

            case SPATIAL -> {
                fromClause = table.database() + "." + table.table();
                xExpr = "geometry.parts[0].coordinates[0].x";
                yExpr = "geometry.parts[0].coordinates[0].y";
            }

            case FLATTEN -> {
                fromClause = table.database() + "." + table.table();
                xExpr = "geometry.x[0]";
                yExpr = "geometry.y[0]";
            }

            default -> throw new IllegalArgumentException();
        }

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
                fromClause,
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

    private static long testQuery2( TableSpec table, GeometryFormat format, boolean indexed) throws IOException {

        switch (format) {
            case WKB -> wkbRegistry.registerDecode();
            case SPATIAL -> spatialRegistry.registerDecode();
            case FLATTEN -> flattenRegistry.registerDecode();
        }

        String fromClause;
        String xExpr;
        String yExpr;


                fromClause = """
                    (
                        SELECT
                            decodeGeometry(geometry) AS geom,
                            geometry
                        FROM %s
                    ) t
                    """.formatted(table.database() + "." + table.table());

                xExpr = "ST_X(geom)";
                yExpr = "ST_Y(geom)";



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
                fromClause,
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


    public static long testDecode(TableSpec table, UDFRegistry<?,?> udfregistry)  {

        String fullName= table.database() + "." + table.table();

        udfregistry.registerDecode();

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

    public static long testDecode2(TableSpec table)  {

        Dataset<Row> t = spark.read()
                .format("iceberg")
                .load(table.database() + "." + table.table());

        long start = System.currentTimeMillis();

        t.selectExpr(
                        "geometry.parts[0].coordinates[0].x - geometry.parts[0].coordinates[0].y as diff"
                )
                .agg(expr("sum(diff)"))
                .show();

        long duration = System.currentTimeMillis() - start;

        System.out.println(
                "Spatial unindexed without decode" + " , query time = "
                        + duration);

        return duration;
    }


    public static long testDecode3(TableSpec table)  {

        Dataset<Row> t = spark.read()
                .format("iceberg")
                .load(table.database() + "." + table.table());

        long start = System.currentTimeMillis();

        t.selectExpr(
                        "geometry.x[0] - geometry.y[0] as diff"
                )
                .agg(expr("sum(diff)"))
                .show();

        long duration = System.currentTimeMillis() - start;

        System.out.println(
                "Flatten unindexed without decode" + " , query time = "
                        + duration);

        return duration;
    }



}


