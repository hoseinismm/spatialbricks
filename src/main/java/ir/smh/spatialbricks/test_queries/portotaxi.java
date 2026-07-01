package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.udf.WKBIndexedParquet;
import ir.smh.spatialbricks.utilities.MultipointToLine;
import ir.smh.spatialbricks.utilities.PowerPlanUtil;
import ir.smh.spatialbricks.core.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.udf.FlattenSpatialParquet;
import ir.smh.spatialbricks.udf.SpatialParquet;
import ir.smh.spatialbricks.udf.UDFRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import static org.apache.spark.sql.functions.*;

public class portotaxi {

    private  static final SparkSession spark = SparkConfig.createSession("../datasets/portotaxi");


    private static final TableSpec wkbUnindexed =
            new TableSpec("wkbUnindexed", "portotaxi", "");

    private static final TableSpec wkbIndexed =
            new TableSpec("wkbIndexed", "portotaxi", "");

    private static final TableSpec silverUnindexed =
            new TableSpec("silverUnindexed", "portotaxi", "");

    private static final TableSpec silverIndexed =
            new TableSpec("silverIndexed", "portotaxi", "");

    private static final TableSpec flattenSilverUnindexed =
            new TableSpec("flattenSilverUnindexed", "portotaxi", "");

    private static final TableSpec flattenSilverIndexed =
            new TableSpec("flattenSilverIndexed", "portotaxi", "");
    public static void main(String[] args) throws Exception {

        PowerPlanUtil.setPowerPlan(PowerPlanUtil.SPARK_TEST);

        try {

            int runs = 10;

            SparkSession spark = createSpark();

            try {




        long[][] results = runBenchmarks( spark, runs);

        writeResults(results, runs);

            }   finally {
                spark.stop();
            }

        } finally {
            PowerPlanUtil.setPowerPlan(PowerPlanUtil.BALANCED);
        }
    }

    private static SparkSession createSpark() {

        SparkSession spark = SparkConfig.createSession("../datasets/portotaxi2");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);

        return spark;
    }

    private static long[][] runBenchmarks(
            SparkSession spark,
            int runs
            ) throws Exception {

        long[][] results = new long[12][runs];

        for (int i = 0; i < runs; i++) {

            System.out.println("Run " + (i + 1));

            results[0][i] = testSpeedIndexed(wkbUnindexed, new WKBIndexedParquet(spark), false);
            results[1][i] = testSpeedIndexed(wkbIndexed, new WKBIndexedParquet(spark), true);
            results[2][i] = testSpeedIndexed(silverUnindexed, new SpatialParquet(spark), false);
            results[3][i] = testSpeedIndexed(silverIndexed, new SpatialParquet(spark), true);
            results[4][i] = testSpeedIndexed(flattenSilverUnindexed, new FlattenSpatialParquet(spark), false);
            results[5][i] = testSpeedIndexed(flattenSilverIndexed, new FlattenSpatialParquet(spark), true);
            results[6][i] = testConvertionToGeometry(wkbUnindexed,new WKBIndexedParquet(spark));
            results[7][i] = testConvertionToGeometry(wkbIndexed,new WKBIndexedParquet(spark));
            results[8][i] = testConvertionToGeometry(silverUnindexed,new SpatialParquet(spark));
            results[9][i] = testConvertionToGeometry(silverIndexed,new SpatialParquet(spark));
            results[10][i] = testConvertionToGeometry(flattenSilverUnindexed,new FlattenSpatialParquet(spark));
            results[11][i] = testConvertionToGeometry(flattenSilverIndexed,new FlattenSpatialParquet(spark));
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
                "WKB Unindexed",
                "WKB Indexed",
                "Spatial Unindexed",
                "Spatial Indexed",
                "Flatten Unindexed",
                "Flatten Indexed"
        };

        try (PrintWriter out = new PrintWriter("benchmarkporto2.csv")) {

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

    public static long testSpeedIndexed( TableSpec silver, UDFRegistry<?,?> udfRegistry, boolean indexed) throws Exception {

        new MultipointToLine(spark, udfRegistry).registerLineFromMultiPoint();

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();

        Column filter = callUDF("ST_X", callUDF("ST_PointN", col("geom"), lit(1))).between(-8.62, -8.56)
                .and(callUDF("ST_Y", callUDF("ST_PointN", col("geom"), lit(1))).between(41.15, 41.19))
                .and(callUDF("ST_Length", col("geom")).gt(0.1));

        if (indexed) {
            filter = filter
                    .and(col("geometry.bbox_partitioning.min_x").lt(-8.56))
                    .and(col("geometry.bbox_partitioning.max_x").gt(-8.62))
                    .and(col("geometry.bbox_partitioning.min_y").lt(41.19))
                    .and(col("geometry.bbox_partitioning.max_y").gt(41.15));
        }

        Dataset<Row> result = spark.read()
                .format("iceberg")
                .load(fullName)
                .withColumn("geom", callUDF("multiPointToLine", col("geometry")))
                .filter(filter)
                .agg(count("*").alias("number"));

        result.show(false);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Querying from  table " +fullName+" in iceberg = "+ duration);

        return  duration;
    }

    public static long testSpeedIndexed2(TableSpec silver, UDFRegistry<?,?> udfRegistry, boolean indexed) throws Exception {

        new MultipointToLine(spark, udfRegistry).registerLineFromMultiPoint();

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();

        Dataset<Row> result = spark.read()
                .format("iceberg")
                .load(fullName);

        if (indexed) {
            result = result.filter(
                    col("geometry.bbox_partitioning.min_x").lt(-8.56)
                            .and(col("geometry.bbox_partitioning.max_x").gt(-8.62))
                            .and(col("geometry.bbox_partitioning.min_y").lt(41.19))
                            .and(col("geometry.bbox_partitioning.max_y").gt(41.15))
            );

        result = result
                .withColumn("geom", callUDF("multiPointToLine", col("geometry")))
                .filter(
                        callUDF("ST_X", callUDF("ST_PointN", col("geom"), lit(1))).between(-8.62, -8.56)
                                .and(callUDF("ST_Y", callUDF("ST_PointN", col("geom"), lit(1))).between(41.15, 41.19))
                                .and(callUDF("ST_Length", col("geom")).gt(0.1))
                )
                .agg(count("*").alias("number"));
        }
        result.show(false);

        long duration = System.currentTimeMillis() - start;

        System.out.println("Querying from  table " +fullName+" in iceberg = "+ duration);

        return  duration;
    }

    public static long testConvertionToGeometry(TableSpec silver, UDFRegistry<?,?> udfRegistry) throws Exception {

        udfRegistry.registerDecode();

        String fullName = silver.database() + "." + silver.table();

        long start = System.currentTimeMillis();

        spark.read()
                .format("iceberg")
                .load(fullName)
                .withColumn("geom", callUDF("decodeGeometry", col("geometry")))
                .select(
                        callUDF(
                                "ST_Distance",
                                callUDF("ST_GeometryN", col("geom"), lit(0)),
                                callUDF(
                                        "ST_GeometryN",
                                        col("geom"),
                                        callUDF("ST_NumGeometries", col("geom")).minus(lit(1))
                                )
                        ).alias("dist")
                )
                .agg(avg("dist"))
                .show();
        System.out.println("Iceberg"+fullName+" decode time = " + (System.currentTimeMillis() - start));

        long duration = System.currentTimeMillis() - start;

        return  duration;
    }
}



