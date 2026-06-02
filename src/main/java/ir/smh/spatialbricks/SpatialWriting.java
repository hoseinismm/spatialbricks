package ir.smh.spatialbricks;


import ir.smh.spatialbricks.encoder.GeometryReader;

import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;


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
            (SparkSession spark, GeometryReader<?> adapter) {
        this.spark = spark;
        this.adapter = adapter;
        this.inputReader = new SpatialInputReader(spark);
        this.bronzeWriter = new BronzeWriter(spark);
        this.silverWriter = new SilverWriter(spark);
        this.bucketService = new BucketService(spark);
    }

    void silverLayerWithoutIndex(TableSpec silver, String inputPath) throws NoSuchTableException {

        UDFRegistry.registerAll(spark,adapter);

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        df= SpatialTransformerForConvertGeometry.transform(df);

        silverWriter.writeSilver(silver, df);
    }

    public void bronzeLayer(TableSpec bronze, String inputPath)
            throws NoSuchTableException, IOException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        bronzeWriter.writeBronze(bronze, df);
    }

    public void silverLayerWithIndex(TableSpec silver, String inputPath,long rowsCapableOfProcessingByDriver, long maxPartitionSize)
            throws NoSuchTableException {

        UDFRegistry.registerAll(spark,adapter);

        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Long totalRowsHint= bucketService.updateBucket(silver);

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        Dataset<Row> transformed = SpatialTransformerForConvertGeometry.transform(df);

        transformed = SpatialTransformerForIndexing.transform(
                transformed,bucketFileName,
                jsc,
                rowsCapableOfProcessingByDriver,
                maxPartitionSize,
                totalRowsHint
        );

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




}










