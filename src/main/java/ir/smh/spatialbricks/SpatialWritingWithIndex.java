package ir.smh.spatialbricks;


import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.CoordinateToGeohashNumericUdfRegistry;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import static ir.smh.spatialbricks.BucketManager2.updateBucket;
import static org.apache.spark.sql.functions.*;


import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class SpatialWritingWithIndex implements Serializable {

    private final SparkSession spark;
    private final GeometryReader<?> adapter;
    private final SpatialInputReader inputReader;
    private final BronzeWriter bronzeWriter;
    private final SilverWriter silverWriter;
    private final BucketService bucketService;


    public SpatialWritingWithIndex(SparkSession spark, GeometryOptions options, GeometryReader<?> adapter) {
        this.spark = spark;
        this.adapter = adapter;
        this.inputReader = new SpatialInputReader(spark);
        this.bronzeWriter = new BronzeWriter(spark);
        this.silverWriter = new SilverWriter(spark);
        this.bucketService = new BucketService(spark);
    }

    void processFileWithoutIndex(TableSpec silver, String inputPath) {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        writeWithoutIndexForSilverLayer(silver, df);
    }

    public void processFileForBronzeLayer(TableSpec bronze, String inputPath)
            throws NoSuchTableException, IOException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        bronzeWriter.writeBronze(bronze, df);
    }

    public void processFileForSilverLayer(TableSpec silver, String inputPath)
            throws NoSuchTableException {

        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        bucketService.updateBucket(silver);

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        Dataset<Row> transformed = SpatialTransformer.transform( df,bucketFileName, jsc );

        silverWriter.writeSilver(silver, transformed);
    }

    private void writeWithoutIndexForSilverLayer(TableSpec silver, String inputPath) throws NoSuchTableException {

        UdfRegistrar.register(spark, adapter);

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        long n1 = df.count();
        System.out.println("Row count n1 = " + n1);


        Dataset<Row> transformed = df
                .withColumn("geometry",
                        callUDF("stringOrGeomToGeometry", col("geometry")))
                .filter(col("geometry").isNotNull());

        long n2 = transformed.count();
        System.out.println("Row count n2 = " + n2);

        silverWriter.writeSilver(silver, df);
    }

    private Dataset<Row> checkGeometryColumnName(Dataset<Row> df) {

        String columnname= Arrays.stream(df.columns())
                .filter(c -> c.equalsIgnoreCase("geometry"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No geometry column found"));

        if (!"geometry".equals(columnname)) {
            df = df.withColumnRenamed(
                    columnname,
                    "geometry");
        }
        return df;
    }

    private void computeGeohashForUnindexedRows(TableSpec silver) {

        String fullName = silver.database() + "." + silver.table();

        spark.sql(String.format(""" 
                EXPLAIN EXTENDED
                UPDATE %s
                SET geometry.geohash_numeric =
                           CASE
                   WHEN geometry.center.x IS NOT NULL
                   AND geometry.center.y IS NOT NULL
                   THEN CoordinateToGeohashNumeric(
                           geometry.center.x,
                           geometry.center.y
                        )
           
                   WHEN size(geometry.parts) > 0
                   AND size(geometry.parts[0].coordinates) > 0
                   THEN CoordinateToGeohashNumeric(
                           geometry.parts[0].coordinates[0].x,
                           geometry.parts[0].coordinates[0].y
                        )
           
                   ELSE NULL
                   END
                   WHERE geometry.partition_number IS NULL;
                """, fullName));
    }


}










