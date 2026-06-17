package ir.smh.spatialbricks.test_queries;

import ir.smh.spatialbricks.TableSpec;
import ir.smh.spatialbricks.config.SparkConfig;
import ir.smh.spatialbricks.encoder.udf.ConvertToSedonaUdfRegistry;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime) // میانگین زمان کل فرآیند را اندازه بگیر
@OutputTimeUnit(TimeUnit.SECONDS) // خروجی نهایی را بر حسب ثانیه نشان بد
@State(Scope.Benchmark) // اصلاح خطا: استفاده از Scope.Benchmark برای نمونه‌سازی یکتا// ک در کل چرخه تست‌ها
@Fork(value = 1, jvmArgs = {
        "-Xms4g",
        "-Xmx4g",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/util=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
})
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.MINUTES) // ۲ بار اجرای طولانی برای گرم شدن کامل JIT کامپایلر و کش اسپارک
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.MINUTES) // ۳ بار تکرار تست اصلی برای میانگین‌گیری نهایی
public class SpatialBrickBenchmark {

    private SparkSession spark;
    private final String path = "../datasets/internet_and_voice_coverage/binaryformat/internet_and_voice_coverage/output_geoparquet/";
    private final TableSpec silver = new TableSpec("indexeddataset", "internet_and_voice_coverage", "");
    private final TableSpec silver2 = new TableSpec("unindexeddataset", "internet_and_voice_coverage", "");

    @Setup(Level.Trial)
    public void setupSpark() {
        // ایجاد یک‌باره محیط اسپارک خارج از تایمر زمان‌گیری بنچمارک
        System.out.println("====== Initializing Spark Session for Benchmark ======");
        spark = SparkConfig.createSession("../datasets/internet_and_voice_coverage");

        SedonaContext.create(spark);
        SedonaSQLRegistrator.registerAll(spark);
        ConvertToSedonaUdfRegistry.registerAll(spark);

        // لود اولیه ویوها در حافظه برای جلوگیری از دخالت زمان Disk I/O در حین تست
        spark.read().parquet(path).createOrReplaceTempView("geoparquet_table");

        try {
            spark.read().format("iceberg").load(silver.database() + "." + silver.table()).createOrReplaceTempView("indexed_table");
            spark.read().format("iceberg").load(silver2.database() + "." + silver2.table()).createOrReplaceTempView("unindexed_table");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @TearDown(Level.Trial)
    public void closeSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

//    @Benchmark
//    public void benchmarkGeoParquetSpeed() {
//        // برای اینکه بنچمارک واقعی باشد، حتما باید با متدی مثل count یا collect دیتا کامیت و پردازش شود.
//        spark.sql("""
//            SELECT COUNT (*) FROM geoparquet_table
//             WHERE
//              ST_XMin(ST_GeomFromWKB(geometry)) > -161   AND
//              ST_XMax(ST_GeomFromWKB(geometry)) < -159   AND
//              ST_YMin(ST_GeomFromWKB(geometry)) > 59     AND
//              ST_YMax(ST_GeomFromWKB(geometry)) < 61
//        """).first();
//    }

//    @Benchmark
//    public void benchmarkBboxIndexingSpeed() {
//        spark.sql("""
//           SELECT COUNT(*)
//           FROM indexed_table
//           WHERE
//              ST_XMin(decodeGeometry(geometry)) > -161   AND
//              ST_XMax(decodeGeometry(geometry)) < -159   AND
//              ST_YMin(decodeGeometry(geometry)) > 59     AND
//              ST_YMax(decodeGeometry(geometry)) < 61
//           AND
//              (
//              geometry.bbox_partitioning.min_x < -159 AND
//              geometry.bbox_partitioning.max_x > -161 AND
//              geometry.bbox_partitioning.min_y < 61 AND
//              geometry.bbox_partitioning.max_y > 59
//              )
//        """).first();
//    }

    @Benchmark
    public void benchmarkDecodeSpatialLakehouseIndexed() {
        spark.sql("SELECT SUM(ST_Area(decodeGeometry(geometry))) FROM indexed_table").first();
    }

    @Benchmark
    public void benchmarkDecodeSpatialLakehouseUnindexed() {
        spark.sql("SELECT SUM(ST_Area(decodeGeometry(geometry))) FROM unindexed_table").first();
    }

    @Benchmark
    public void benchmarkDecodeGeoparquet() {
        spark.sql("""
            SELECT SUM(ST_Area(ST_GeomFromWKB(geometry))) FROM geoparquet_table
        """).first();
    }



    // متد اصلی برای استارت زدن فرآیند بنچمارک JMH
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(SpatialBrickBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}