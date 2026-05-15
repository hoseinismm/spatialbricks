package ir.smh.spatialbricks.config;

import org.apache.spark.sql.SparkSession;

public class SparkConfig {
    public static SparkSession createSession(String warehousePath) {
        return SparkSession.builder()
                .appName("Spatial-Lakehouse-Writer")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.sedona.sql.SedonaSqlExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", warehousePath)
                .config("spark.jars.packages", String.join(",", new String[]{
                        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.2",
                        "org.apache.sedona:sedona-spark-shaded-3.5_2.13:1.7.2"
                }))
                .config("spark.eventLog.enabled", "false")
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.sql.streaming.metricsEnabled", "false")
                .master("local[*]")
                .getOrCreate();
    }
}

