package ir.smh.spatialbricks.core;

import org.apache.sedona.core.formatMapper.GeoJsonReader;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class SpatialInputReader_version3 {

    private final SparkSession spark;


    public SpatialInputReader_version3(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> read(
            String inputPath
           ) {
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        String path = inputPath.toLowerCase();

        if (path.endsWith(".json") || path.endsWith(".geojson")) {
            return Adapter.toDf(
                    GeoJsonReader.readToGeometryRDD(
                            jsc,
                            inputPath),
                    spark);
        }

        if (path.endsWith(".parquet")) {
            return spark.read().parquet(inputPath);
        }

        if (path.endsWith(".csv")) {
            return spark.read().csv(inputPath);
        }

        throw new IllegalArgumentException(
                "Unsupported file format: " + inputPath);
    }
}