package ir.smh.spatialbricks.core;

import ir.smh.spatialbricks.encoder.converttogeometry.GeometryReader;
import ir.smh.spatialbricks.udf.SpatialParquet;
import ir.smh.spatialbricks.udf.UDFRegistry;
import ir.smh.spatialbricks.encoder.converttogeometry.WKBReaderAdapter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;


public class SpatialWriting implements Serializable {

    private final SparkSession spark;
    private final GeometryReader<?> adapter;
    private final SpatialInputReader_version4 inputReader;
    private final SilverBboxWriter silverBboxWriter;
    private final BucketServiceForBboxIndexing bucketServiceForBboxIndexing;
    private final UDFRegistry<?,?> udfRegistry;

    public SpatialWriting
            (SparkSession spark, GeometryReader<?> adapter, UDFRegistry<?,?> udfRegistry) {
        this.spark = spark;
        this.adapter = adapter;
        this.inputReader = new SpatialInputReader_version4(spark);
        this.silverBboxWriter = new SilverBboxWriter(spark);
        this.bucketServiceForBboxIndexing = new BucketServiceForBboxIndexing(spark);
        this.udfRegistry = udfRegistry;
    }

    public SpatialWriting(SparkSession spark) {
        this( null,new SpatialParquet (spark));
    }

    public SpatialWriting(SparkSession spark, GeometryReader<?> adapter) {
        this(spark, adapter, new SpatialParquet (spark));
    }

    public SpatialWriting(SparkSession spark, UDFRegistry<?,?> udfRegistry) {
        this(spark,  new WKBReaderAdapter(), udfRegistry);
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
    ) throws NoSuchTableException, ParseException {

        udfRegistry.registerGeometryUdf(adapter);

        Dataset<Row> transformed = SpatialTransformerForConvertGeometry.transform(df, false);

        silverBboxWriter.writeSilver(silver, transformed);
    }

    public void silverLayerWithBboxIndexing(
            TableSpec silver,
            String inputPath,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize)
            throws Exception {

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
            throws NoSuchTableException, ParseException {

        udfRegistry.registerGeometryUdf(adapter);

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
                        .transform(df, true);

        BucketManagerForBboxIndexing.Bucket rootBucket =
                BucketManagerForBboxIndexing.computeBucketBorders(
                        spark,
                        transformed,
                        silver,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        totalRowsHint,
                        udfRegistry
                );

        transformed =
                SpatialTransformerForBboxIndexing.transform(
                        rootBucket,
                        transformed,
                        jsc,
                        udfRegistry
                );

        silverBboxWriter.writeSilver(
                silver,
                transformed
        );

        updateOrCreateBucketFile(spark, rootBucket, silver);

    }

    private void updateOrCreateBucketFile(SparkSession spark,BucketManagerForBboxIndexing.Bucket rootBucket, TableSpec silver) throws NoSuchTableException, ParseException {

        String fullName = silver.database() + "." + silver.table();

        Table table = Spark3Util.loadIcebergTable(
                spark,
                fullName);

        Snapshot snapshot = table.currentSnapshot();

        if (snapshot == null) {
            throw new IllegalStateException(
                    "Table " + table.name() + " has no current snapshot.");
        }

        rootBucket.snapshot = snapshot.snapshotId();

        Path bucketPath = Paths.get(
                silver.path(),
                String.format(
                        "bucket_%s_%s.gz",
                        silver.database(),
                        silver.table()
                )
        );
        BucketManagerForBboxIndexing.saveBucket(rootBucket,bucketPath.toString());

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

//      transformed=transformed.cache();

        Long totalRowsHint =
                bucketServiceForBboxIndexing.updateBucket(
                        silver
                );

        BucketManagerForBboxIndexing.Bucket rootBucket =
                BucketManagerForBboxIndexing.computeBucketBorders(
                        spark,
                        transformed,
                        silver,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        totalRowsHint,
                        udfRegistry
                );

        transformed =
                SpatialTransformerForBboxIndexing.transform(
                        rootBucket,
                        transformed,
                        jsc,
                        udfRegistry
                );

        silverBboxWriter.writeSilver(
                silver,
                transformed
        );

        updateOrCreateBucketFile(spark, rootBucket, silver);
    }

    public void customWriterWithoutBboxIndex(
            TableSpec silver,
            String inputPath,
            String xColumn, String yColumn)
            throws NoSuchTableException, ParseException {

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










