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
import java.util.Objects;

public class SpatialWriting implements Serializable {

    private final SparkSession spark;
    private final GeometryReader<?> adapter;
    private final SpatialInputReader inputReader;
    private final BronzeWriter bronzeWriter;
    private final SilverGeohashWriter silverGeohashWriter;
    private final SilverBboxWriter silverBboxWriter;
    private final BucketServiceForGeohashindexing bucketServiceForGeohashindexing;
    private final BucketServiceForBboxIndexing bucketServiceForBboxIndexing;


    public SpatialWriting
            (SparkSession spark, GeometryReader<?> adapter) {
        this.spark = spark;
        this.adapter = adapter;
        this.inputReader = new SpatialInputReader(spark);
        this.bronzeWriter = new BronzeWriter(spark);
        this.silverGeohashWriter = new SilverGeohashWriter(spark);
        this.silverBboxWriter = new SilverBboxWriter(spark);
        this.bucketServiceForGeohashindexing = new BucketServiceForGeohashindexing(spark);
        this.bucketServiceForBboxIndexing = new BucketServiceForBboxIndexing(spark);

    }

    void silverLayerWithoutIndex(TableSpec silver, String inputPath, String typeOfPartitioning) throws NoSuchTableException {

        UDFRegistry.registerAll(spark,adapter);

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        df= SpatialTransformerForConvertGeometry.transform(df);

        if ("bbox".equals(typeOfPartitioning)) {
            silverBboxWriter.writeSilver(silver, df);
        } else if ("geohash".equals(typeOfPartitioning)) {
            silverGeohashWriter.writeSilver(silver, df);
        }
    }

    public void bronzeLayer(TableSpec bronze, String inputPath)
            throws NoSuchTableException, IOException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        bronzeWriter.writeBronze(bronze, df);
    }

    public void silverLayerWithgeohashIndexing(TableSpec silver, String inputPath,long rowsCapableOfProcessingByDriver, long maxPartitionSize)
            throws NoSuchTableException {


        UDFRegistry.registerAll(spark,adapter);

        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Long totalRowsHint= bucketServiceForGeohashindexing.updateBucket(silver);

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        Dataset<Row> transformed = SpatialTransformerForConvertGeometry.transform(df);

        transformed = SpatialTransformerForGeohashIndexing.transform(
                transformed,bucketFileName,
                jsc,
                rowsCapableOfProcessingByDriver,
                maxPartitionSize,
                totalRowsHint
        );

        silverGeohashWriter.writeSilver(silver, transformed);
    }

    public void silverLayerWithbboxIndexing(TableSpec silver, String inputPath,long rowsCapableOfProcessingByDriver, long maxPartitionSize)
            throws NoSuchTableException {


        UDFRegistry.registerAll(spark,adapter);

        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Long totalRowsHint= bucketServiceForBboxIndexing.updateBucket(silver);

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        Dataset<Row> transformed = SpatialTransformerForConvertGeometry.transform(df);

        transformed = SpatialTransformerForBboxIndexing.transform(
                transformed,bucketFileName,
                jsc,
                rowsCapableOfProcessingByDriver,
                maxPartitionSize,
                totalRowsHint
        );

        silverBboxWriter.writeSilver(silver, transformed);
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










