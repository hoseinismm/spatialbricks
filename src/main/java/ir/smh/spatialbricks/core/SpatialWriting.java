package ir.smh.spatialbricks.core;

import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.SpatialParquet;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import ir.smh.spatialbricks.encoder.converttogeometry.WKBReaderAdapter;
import org.apache.spark.sql.Row;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.Serializable;
import java.util.Arrays;


public class SpatialWriting implements Serializable {

    private final SparkSession spark;
    private final GeometryReader<?> adapter;
    private final SpatialInputReader inputReader;
    private final BronzeWriter bronzeWriter;
    private final SilverBboxWriter silverBboxWriter;
    private final BucketServiceForBboxIndexing bucketServiceForBboxIndexing;
    private final UDFRegistry udfRegistry;

    public SpatialWriting
            (SparkSession spark, GeometryReader<?> adapter, UDFRegistry udfRegistry) {
        this.spark = spark;
        this.adapter = adapter;
        this.inputReader = new SpatialInputReader(spark);
        this.bronzeWriter = new BronzeWriter(spark);
        this.silverBboxWriter = new SilverBboxWriter(spark);
        this.bucketServiceForBboxIndexing = new BucketServiceForBboxIndexing(spark);
        this.udfRegistry = udfRegistry;

    }

    public SpatialWriting(SparkSession spark) {
        this(spark, null,new SpatialParquet ());
    }

    public SpatialWriting(SparkSession spark, GeometryReader<?> adapter) {
        this(spark, adapter, new SpatialParquet ());
    }

    public SpatialWriting(SparkSession spark, UDFRegistry udfRegistry) {
        this(spark,  new WKBReaderAdapter(), udfRegistry);
    }


    public void bronzeLayer(TableSpec bronze, String inputPath)
            throws Exception {

        Dataset<Row> df = inputReader.read(inputPath);

        df = checkGeometryColumnName(df);

        bronzeWriter.writeBronze(bronze, df);
    }

    public void bronzeLayerBinary(TableSpec bronze, String inputPath)
            throws Exception {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = inputReader.read(inputPath);

        df = checkGeometryColumnName(df);

        bronzeWriter.writeBronzeBinary(bronze, df);
    }

    public void silverLayerWithoutBboxIndexing(
            TableSpec silver,
            String inputPath
            ) throws Exception {

        Dataset<Row> df =
                inputReader.read(inputPath);
//        df.printSchema();
//        System.in.read();

        silverLayerWithoutBboxIndexing(
                silver,
                df
        );
    }

    public void silverLayerWithoutBboxIndexing(
            TableSpec silver,
            Dataset<Row> df
    )   throws NoSuchTableException {

        udfRegistry.registerGeometryUdf(spark, adapter);

        Dataset<Row> transformed = SpatialTransformerForConvertGeometry.transform(df);

        silverBboxWriter.writeSilver(silver, transformed);
    }

    public void silverLayerWithBboxIndexing(
            TableSpec silver,
            String inputPath,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize)
            throws Exception {

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(
                        spark.sparkContext());

        Dataset<Row> df =
                inputReader.read(inputPath);

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

        udfRegistry.registerGeometryUdf(spark, adapter);

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
                        silver,
                        jsc,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        totalRowsHint, udfRegistry
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
            throws Exception {

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(
                        spark.sparkContext());

        Dataset<Row> df =
                inputReader.read(inputPath);

        Dataset<Row> transformed = udfRegistry.addPointGeometryColumn(df, xColumn, yColumn, "geometry");

        Long totalRowsHint =
                bucketServiceForBboxIndexing.updateBucket(
                        silver
                );

        transformed =
                SpatialTransformerForBboxIndexing.transform(
                        transformed,
                        silver,
                        jsc,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        totalRowsHint,
                        udfRegistry
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

        Dataset<Row> transformed = udfRegistry.addPointGeometryColumn(df, xColumn, yColumn, "geometry");

        silverBboxWriter.writeSilver(
                silver,
                transformed
        );

    }
}










