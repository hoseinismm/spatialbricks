package ir.smh.spatialbricks.config;

import org.apache.spark.sql.SparkSession;

public class SparkConfigLocal {

    public static SparkSession createSession(String warehousePath) {
        return SparkSession.builder()
                .appName("Spatial-Lakehouse-Writer")

                // ================= MEMORY =================
                .config("spark.driver.memory", "12g")
                .config("spark.driver.maxResultSize", "4g")
                .config("spark.executor.memory", "8g")

                .config("spark.memory.fraction", "0.8")
                .config("spark.memory.storageFraction", "0.3")

                // ================= OFFHEAP =================
                .config("spark.memory.offHeap.enabled", "true")
                .config("spark.memory.offHeap.size", "2g")

                // ================= PERFORMANCE =================
                .config("spark.sql.shuffle.partitions", "50")
                .config("spark.default.parallelism", "50")
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
//                .config("spark.sql.files.maxPartitionBytes", "32m")
//                .config("spark.sql.parquet.blockSize", "32m")

                // ================= SEDONA + ICEBERG =================
                .config("spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
                                "org.apache.sedona.sql.SedonaSqlExtensions")

                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", warehousePath)

                // ================= PACKAGES =================
                .config("spark.jars.packages", String.join(",", new String[]{
                        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.2",
                        "org.apache.sedona:sedona-spark-shaded-3.5_2.13:1.7.2"
                }))

                .master("local[4]")

                .getOrCreate();
    }
}