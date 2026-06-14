package ir.smh.spatialbricks;
import ir.smh.spatialbricks.encoder.GeometryBuilder;

import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.spark.sql.Row;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
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
    private final SilverBboxWriter silverBboxWriter;
    private final BucketServiceForBboxIndexing bucketServiceForBboxIndexing;

    public SpatialWriting
            (SparkSession spark, GeometryReader<?> adapter) {
        this.spark = spark;
        this.adapter = adapter;
        this.inputReader = new SpatialInputReader(spark);
        this.bronzeWriter = new BronzeWriter(spark);
        this.silverBboxWriter = new SilverBboxWriter(spark);
        this.bucketServiceForBboxIndexing = new BucketServiceForBboxIndexing(spark);
    }

    public SpatialWriting(SparkSession spark) {
        this(spark, null);
    }



    public void bronzeLayer(TableSpec bronze, String inputPath)
            throws NoSuchTableException, IOException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath, jsc);

        df = checkGeometryColumnName(df);

        bronzeWriter.writeBronze(bronze, df);
    }

    public void silverLayerWithoutBboxIndexing(
            TableSpec silver,
            String inputPath
            )   throws NoSuchTableException {

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(
                        spark.sparkContext());

        Dataset<Row> df =
                inputReader.read(inputPath, jsc);

        silverLayerWithoutBboxIndexing(
                silver,
                df
        );
    }

    public void silverLayerWithoutBboxIndexing(
            TableSpec silver,
            Dataset<Row> df
    )   throws NoSuchTableException {

        Dataset<Row> transformed = SpatialTransformerForConvertGeometry.transform(df);

        silverBboxWriter.writeSilver(silver, transformed);
    }




    public void silverLayerWithBboxIndexing(
            TableSpec silver,
            String inputPath,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize)
            throws NoSuchTableException {

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(
                        spark.sparkContext());

        Dataset<Row> df =
                inputReader.read(inputPath, jsc);

        silverLayerWithBboxIndexing(
                silver,
                df,
                rowsCapableOfProcessingByDriver,
                maxPartitionSize
        );
    }

    public void silverLayerWithBboxIndexing(
            TableSpec silver,
            Dataset<Row> df,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize)
            throws NoSuchTableException {

        UDFRegistry.registerAll(spark, adapter);

        String bucketFileName =
                "bucket_"
                        + silver.database()
                        + "_"
                        + silver.table()
                        + ".gz";

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(
                        spark.sparkContext());

        Long totalRowsHint =
                bucketServiceForBboxIndexing.updateBucket(
                        silver
                );

        df = checkGeometryColumnName(df);

        Dataset<Row> transformed =
                SpatialTransformerForConvertGeometry
                        .transform(df);

        transformed =
                SpatialTransformerForBboxIndexing.transform(
                        transformed,
                        bucketFileName,
                        jsc,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        totalRowsHint
                );

        silverBboxWriter.writeSilver(
                silver,
                transformed
        );
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

    public void customWriter(
            TableSpec silver,
            String inputPath,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize, String xColumn, String yColumn)
            throws NoSuchTableException {

        String bucketFileName =
                "bucket_"
                        + silver.database()
                        + "_"
                        + silver.table()
                        + ".gz";

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(
                        spark.sparkContext());

        Dataset<Row> df =
                inputReader.read(inputPath, jsc);

        Dataset<Row> transformed = GeometryBuilder.addPointGeometryColumn(df, xColumn, yColumn, "geometry");

        Long totalRowsHint =
                bucketServiceForBboxIndexing.updateBucket(
                        silver
                );

        transformed =
                SpatialTransformerForBboxIndexing.transform(
                        transformed,
                        bucketFileName,
                        jsc,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        totalRowsHint
                );

        silverBboxWriter.writeSilver(
                silver,
                transformed
        );
    }

    public void customWriterWithoutBboxIndex(
            TableSpec silver,
            String inputPath,
            String xColumn, String yColumn)
            throws NoSuchTableException {

        Dataset<Row> df;

        if (inputPath.endsWith(".parquet")) {
            df = spark.read().parquet(inputPath);
        } else if (inputPath.endsWith(".csv")) {
            df = spark.read().csv(inputPath);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported file format: " + inputPath);
        }

        Dataset<Row> transformed = GeometryBuilder.addPointGeometryColumn(df, xColumn, yColumn, "geometry");

        silverBboxWriter.writeSilver(
                silver,
                transformed
        );

    }
}










