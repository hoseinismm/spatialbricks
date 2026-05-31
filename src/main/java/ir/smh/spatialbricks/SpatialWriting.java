package ir.smh.spatialbricks;


import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;


import static org.apache.spark.sql.functions.*;


import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class SpatialWriting implements Serializable {

    private final SparkSession spark;
    private final GeometryReader<?> adapter;
    private final SpatialInputReader inputReader;
    private final BronzeWriter bronzeWriter;
    private final SilverWriter silverWriter;
    private final BucketService bucketService;


    public SpatialWriting
            (SparkSession spark, GeometryOptions options, GeometryReader<?> adapter) {
        this.spark = spark;
        this.adapter = adapter;
        this.inputReader = new SpatialInputReader(spark);
        this.bronzeWriter = new BronzeWriter(spark);
        this.silverWriter = new SilverWriter(spark);
        this.bucketService = new BucketService(spark);
    }

    void silverLayerWithoutIndex(TableSpec silver, String inputPath) throws NoSuchTableException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        silverWriter.writeSilver(silver, df);
    }

    public void bronzeLayer(TableSpec bronze, String inputPath)
            throws NoSuchTableException, IOException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        bronzeWriter.writeBronze(bronze, df);
    }

    public void silverLayerWithIndex(TableSpec silver, String inputPath)
            throws NoSuchTableException {

        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        bucketService.updateBucket(silver);

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        Dataset<Row> transformed = SpatialTransformer.transform( df,bucketFileName, jsc );

        silverWriter.writeSilver(silver, transformed);
    }

    private Dataset<Row> checkGeometryColumnName(Dataset<Row> df) {

        String columnname= Arrays.stream(df.columns())
                .filter(c -> c.equalsIgnoreCase("geometry"))
                .findFirst()
                .orElseThrow(() -> new  IllegalArgumentException("No geometry column found"));

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










