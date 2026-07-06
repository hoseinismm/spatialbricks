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


public class PipelineExecutor implements Serializable {

    private final SparkSession spark;
    private final GeometryReader<?> adapter;
    private final SpatialReader_version4 spatialReader;
    private final TableWriter tableWriter;
    private final BucketService bucketService;
    private final UDFRegistry<?,?> udfRegistry;

    public PipelineExecutor
            (SparkSession spark, GeometryReader<?> adapter, UDFRegistry<?,?> udfRegistry) {
        this.spark = spark;
        this.adapter = adapter;
        this.spatialReader = new SpatialReader_version4(spark);
        this.tableWriter = new TableWriter(spark);
        this.bucketService = new BucketService(spark);
        this.udfRegistry = udfRegistry;
    }

    public PipelineExecutor(SparkSession spark) {
        this( null,new SpatialParquet (spark));
    }

    public PipelineExecutor(SparkSession spark, GeometryReader<?> adapter) {
        this(spark, adapter, new SpatialParquet (spark));
    }

    public PipelineExecutor(SparkSession spark, UDFRegistry<?,?> udfRegistry) {
        this(spark,  new WKBReaderAdapter(), udfRegistry);
    }

    public void AddDataWithoutIndexing(
            TableSpec silver,
            String inputPath
            ) throws Exception {

        Dataset<Row> df =
                spatialReader.read(inputPath);
//        df.printSchema();
//        System.in.read();

        AddDataWithoutIndexing(
                silver,
                df
        );
    }

    public void AddDataWithoutIndexing(
            TableSpec silver,
            Dataset<Row> df
    ) throws NoSuchTableException, ParseException {

        udfRegistry.registerGeometryUdf(adapter);

        Dataset<Row> transformed = GeometryTransformer.transform(df, false);

        tableWriter.writeSilver(silver, transformed);
    }

    public void AddDataWithIndexing(
            TableSpec silver,
            String inputPath,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize)
            throws Exception {

        Dataset<Row> df =
                spatialReader.read(inputPath);

        AddDataWithIndexing(
                silver,
                df,
                rowsCapableOfProcessingByDriver,
                maxPartitionSize
        );
    }

    public void AddDataWithIndexing(
            TableSpec silver,
            Dataset<Row> df,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize)
            throws NoSuchTableException, ParseException {

        udfRegistry.registerGeometryUdf(adapter);

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(
                        spark.sparkContext());

        bucketService.updateBucket(silver);

        df = checkGeometryColumnName(df);

        Dataset<Row> transformed =
                GeometryTransformer
                        .transform(df, true);

        BucketManager.Bucket rootBucket =
                BucketManager.computeBucketBorders(
                        spark,
                        transformed,
                        silver,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        udfRegistry
                );

        transformed =
                BboxIndexing.transform(
                        rootBucket,
                        transformed,
                        jsc,
                        udfRegistry
                );

        tableWriter.writeSilver(
                silver,
                transformed
        );

        updateOrCreateBucketFile(spark, rootBucket, silver);

    }

    private void updateOrCreateBucketFile(SparkSession spark, BucketManager.Bucket rootBucket, TableSpec silver) throws NoSuchTableException, ParseException {

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
        BucketManager.saveBucket(rootBucket,bucketPath.toString());

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

    public void xyToPintTableWithIndexing(
            TableSpec silver,
            String inputPath,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize, String xColumn, String yColumn)
            throws Exception {

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(
                        spark.sparkContext());

        Dataset<Row> df =
                spatialReader.read(inputPath);

        Dataset<Row> transformed = udfRegistry.addPointGeometryColumn(df, xColumn, yColumn, "geometry");

//      transformed=transformed.cache();

        bucketService.updateBucket(silver);

        BucketManager.Bucket rootBucket =
                BucketManager.computeBucketBorders(
                        spark,
                        transformed,
                        silver,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,                        
                        udfRegistry
                );

        transformed =
                BboxIndexing.transform(
                        rootBucket,
                        transformed,
                        jsc,
                        udfRegistry
                );

        tableWriter.writeSilver(
                silver,
                transformed
        );

        updateOrCreateBucketFile(spark, rootBucket, silver);
    }

    public void xyToPointTableWithoutIndexing(
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

        tableWriter.writeSilver(
                silver,
                transformed
        );

    }
}










