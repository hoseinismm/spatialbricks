package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.udf.SpatialParquet;
import ir.smh.spatialbricks.udf.UDFRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import static org.apache.spark.sql.functions.*;

public class portotaxi {

    public static void main(String[] args) throws Exception {

        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            final int runs = 10;

            SparkSession spark = createSpark();

            try {

        String path = "../datasets/portotaxi/porto_taxi_chunk_*.parquet";

        TableSpec silverUnindexed = new TableSpec("silverUnindexed", "portotaxi", "");
        TableSpec silverIndexed = new TableSpec("silverIndexed", "portotaxi", "");
        TableSpec flattenSilverUnindexed = new TableSpec("flattenSilverUnindexed", "portotaxi", "");
        TableSpec flattenSilverIndexed = new TableSpec("flattenSilverIndexed", "portotaxi", "");


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

        SparkSession spark = SparkConfig.createSession("../datasets/portotaxi");

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

            results[0][i] = testSpeedGeoParquet(spark, path);
            results[1][i] = testSpeedBboxIndexed(spark, silverIndexed);
            results[2][i] = testSpeedBboxUnindexed(spark, silverUnindexed);
            results[3][i] = testSpeedFlattenBboxIndexed(spark, flattenSilverIndexed);
            results[4][i] = testSpeedFlattenBboxUnindexed(spark, flattenSilverUnindexed);
            results[5][i] = testConvertionToGeometryForGeoparquet(spark, path);
            results[6][i] = testConvertionToGeometryForSpatialParquet(spark, silverUnindexed);
            results[7][i] = testConvertionToGeometryForSpatialParquet(spark, silverIndexed);
            results[8][i] = testConvertionToGeometryForFlattenSpatialParquet(spark,flattenSilverUnindexed);
            results[9][i] = testConvertionToGeometryForFlattenSpatialParquet(spark,flattenSilverIndexed);
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

        try (PrintWriter out = new PrintWriter("benchmark.csv")) {

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

    public static long testSpeedGeoParquet(SparkSession spark, String path) throws NoSuchTableException {

            String table = "a.b";
//          ایجاد یک جدول برای فیتر ردیفهای کمتر از دو نقطه چون بدون ذخیره سازی جواب نمیداد
//            Dataset<Row> t1 = spark.read()
//                    .parquet(path)
//                    .withColumn("geom", expr("ST_GeomFromWKB(geometry)"))
//                    .filter(expr("ST_NumGeometries(geom) >= 2"))
//                    .withColumn("geom", expr("ST_AsBinary(geom)"))

//                    .select("geom");
//
//            IcebergTableCreator.createIcebergTableFromSchema(
//                    spark,  t1.schema(), "a", "b"
//            );
//            System.out.println("table is created");
//
//            t1.writeTo(table).append();
//            System.out.println("data appended");
//

        long start = System.currentTimeMillis();

            Dataset<Row> t2 = spark.read()
                    .format("iceberg")
                    .load(table)
                    .withColumn("geom", expr("ST_GeomFromWKB(geom)"));

            t2.createOrReplaceTempView("table");



            spark.sql("""
            WITH g AS (
                            SELECT
                                ST_LineFromMultiPoint(geom) AS line
                            FROM table
                        )
                        SELECT COUNT(*)
                        FROM g
                        WHERE
                            ST_X(ST_PointN(line, 1)) BETWEEN -8.62 AND -8.56
                        AND ST_Y(ST_PointN(line, 1)) BETWEEN 41.15 AND 41.19
                        AND ST_Length(line) > 0.1;
        """).show(false);

        System.out.println("Querying from geoparquet file time = " + (System.currentTimeMillis() - start));
        return  (System.currentTimeMillis() - start);
    }

    public static long testSpeedBboxUnindexed(SparkSession spark, TableSpec silver) throws Exception {

        UDFRegistry udfRegistry= new SpatialParquet();
        udfRegistry.registerDecode(spark);

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
                  ST_Length(geom) > 0.1
                """).show(false);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Querying from  table " +fullName+" in iceberg = "+ duration);

        return  (System.currentTimeMillis() - start);
    }

    public static long testSpeedBboxIndexed(SparkSession spark, TableSpec silver) throws Exception {

        UDFRegistry udfRegistry= new SpatialParquet();
        udfRegistry.registerDecode(spark);

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
                AND
                  ST_Length(geom) > 0.1
                """).show(false);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Querying from  table " +fullName+" in iceberg = "+ duration);

        return  (System.currentTimeMillis() - start);
    }

    public static long testSpeedFlattenBboxIndexed(SparkSession spark, TableSpec silver) throws Exception {

        UDFRegistry udfRegistry= new FlattenSpatialParquet();
        udfRegistry.registerDecode(spark);

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();

        Dataset<Row> t1 = spark.read()
                .format("iceberg")
                .load(fullName)
                .filter(expr("size(geometry.x) >= 2"))
                .withColumn("geom", callUDF("decodeGeometry", col("geometry")))
                .withColumn("geom",  expr("ST_LineFromMultiPoint(geom)"));

        t1.createOrReplaceTempView("table");

        spark.sql("""
                SELECT
                    COUNT(*) AS number
                FROM table
                WHERE
                      (
                      geometry.x[0] BETWEEN -8.62 AND -8.56
                      AND
                      geometry.y[0] BETWEEN 41.15 AND 41.19
                      )
                AND
                      (
                      geometry.bbox_partitioning.min_x < -8.56 AND
                      geometry.bbox_partitioning.max_x > -8.62 AND
                      geometry.bbox_partitioning.min_y < 41.19 AND
                      geometry.bbox_partitioning.max_y > 41.15
                      )
                AND
                  ST_Length(geom) > 0.1
                """).show(false);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Querying from  table " +fullName+" in iceberg = "+ duration);

        return  (System.currentTimeMillis() - start);
    }

    public static long testSpeedFlattenBboxUnindexed(SparkSession spark, TableSpec silver) throws Exception {

        UDFRegistry udfRegistry= new FlattenSpatialParquet();
        udfRegistry.registerDecode(spark);

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();

        Dataset<Row> t1 = spark.read()
                .format("iceberg")
                .load(fullName)
                .filter(expr("size(geometry.x) >= 2"))
                .withColumn("geom", callUDF("decodeGeometry", col("geometry")))
                .withColumn("geom",  expr("ST_LineFromMultiPoint(geom)"));

        t1.createOrReplaceTempView("table");

        spark.sql("""
                SELECT
                    COUNT(*) AS number
                FROM table
                WHERE
                      (
                      geometry.x[0] BETWEEN -8.62 AND -8.56
                      AND
                      geometry.y[0] BETWEEN 41.15 AND 41.19
                      )
                AND
                  ST_Length(geom) > 0.1
                """).show(false);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Querying from  table " +fullName+" in iceberg = "+ duration);

        return  (System.currentTimeMillis() - start);
    }

    public static long testConvertionToGeometryForSpatialParquet(SparkSession spark, TableSpec silver) throws Exception {

        UDFRegistry udfRegistry= new SpatialParquet();
        udfRegistry.registerDecode(spark);

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();

        Dataset<Row> t = spark.read()
                .format("iceberg")
                .load(fullName).withColumn(
                        "geom",
                        expr("decodeGeometry(geometry)")
                );

        t.selectExpr(
                "ST_Distance(ST_GeometryN(geom, 0), ST_GeometryN(geom, ST_NumGeometries(geom)-1)) AS dist"
                )
                .agg(expr("avg(dist)"))
                .show();
        System.out.println("Iceberg"+fullName+" decode time = " + (System.currentTimeMillis() - start));
        return  (System.currentTimeMillis() - start);
    }

    public static long testConvertionToGeometryForFlattenSpatialParquet(SparkSession spark, TableSpec silver) throws Exception {

        UDFRegistry udfRegistry= new FlattenSpatialParquet();
        udfRegistry.registerDecode(spark);

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();

        Dataset<Row> t = spark.read()
                .format("iceberg")
                .load(fullName).withColumn(
                        "geom",
                        expr("decodeGeometry(geometry)")
                );

        t.selectExpr(
                        "ST_Distance(ST_GeometryN(geom, 0), ST_GeometryN(geom, ST_NumGeometries(geom)-1)) AS dist"
                )
                .agg(expr("avg(dist)"))
                .show();
        System.out.println("Iceberg"+fullName+" decode time = " + (System.currentTimeMillis() - start));
        return  (System.currentTimeMillis() - start);
    }

    public static long testConvertionToGeometryForGeoparquet(SparkSession spark, String path) throws Exception {

        long start = System.currentTimeMillis();

        Dataset<Row> t = spark.read()
                .parquet(path)
                .withColumn("geom", expr("ST_GeomFromWKB(geometry)"));

        Dataset<Row> bigger = t;

        t.selectExpr(
                        "ST_Distance(ST_GeometryN(geom, 0), ST_GeometryN(geom, ST_NumGeometries(geom)-1)) AS dist"
                )
                .agg(expr("avg(dist)"))
                .show();


        System.out.println(
                "GeoParquet decode time = "
                        + (System.currentTimeMillis() - start));
        return  (System.currentTimeMillis() - start);

    }
}


