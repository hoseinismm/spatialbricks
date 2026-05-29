package ir.smh.spatialbricks;

import ir.smh.spatialbricks.createsql.IcebergTableCreator;
import ir.smh.spatialbricks.createsql.IcebergTableCreatorWithPartitioning;
import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.CoordinateToGeohashNumericUdfRegistry;
import ir.smh.spatialbricks.encoder.udf.GeohashToIntegerUdfRegistry;
import ir.smh.spatialbricks.encoder.udf.SparkUdfs;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.sedona.core.formatMapper.GeoJsonReader;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SpatialWritingWithIndex implements Serializable {

    private final SparkSession spark;
    private final GeometryOptions options;
    private final GeometryReader<?> adapter;



    public SpatialWritingWithIndex(SparkSession spark, GeometryOptions options, GeometryReader<?> adapter) {
        this.spark = spark;
        this.options = options;
        this.adapter = adapter;



    }

    public void processFile(TableSpec bronze, TableSpec silver, String inputPath)
            throws NoSuchTableException, IOException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = readInput(inputPath, jsc);

        writeBronze(bronze, df);

        updateBucket(silver);

        writeSilver(silver,jsc, df);


    }

    // ---------------------------
    // read data from file
    // ---------------------------

    private Dataset<Row> readInput(String inputPath, JavaSparkContext jsc) {

        String path = inputPath.toLowerCase();

        if (path.endsWith(".json") || path.endsWith(".geojson")) {
            return Adapter.toDf(
                    GeoJsonReader.readToGeometryRDD(jsc, inputPath),
                    spark
            );
        }

        if (path.endsWith(".parquet")) {
            return spark.read().parquet(inputPath);
        }

        if (path.endsWith(".csv")) {
            return spark.read().csv(inputPath);
        }

        throw new IllegalArgumentException("Unsupported file format: " + inputPath);
    }

    // ---------------------------
    // create raw data
    // ---------------------------

    private void writeBronze(TableSpec bronze, Dataset<Row> df) throws NoSuchTableException {

        Dataset<Row> output = df;

        if (Arrays.asList(df.columns()).contains("geometry")) {
            output = df.withColumn("geometry", expr("ST_AsGeoJSON(geometry)"));
        }

        createOrAppend(
                output,
                bronze.database(),
                bronze.table()
        );
    }


    private void createOrAppend(
            Dataset<Row> df,
            String database,
            String table
    ) throws NoSuchTableException {

        String fullName = database + "." + table;

        boolean exists = spark.catalog().tableExists(fullName);

        if (!exists) {

            IcebergTableCreator.createIcebergTableFromSchema(
                    spark,
                    df.schema(),
                    database,
                    table
            );

            df.writeTo(fullName).append();
            return;
        }

        // read existing schema
        StructType tableSchema = spark.table(fullName).schema();

        if (!tableSchema.equals(df.schema())) {
            throw new RuntimeException(
                    "Schema mismatch between incoming data and table: " + fullName
            );
        }

        df.writeTo(fullName).append();
    }

    // ---------------------------
    // ELT process
    // ---------------------------

    private Dataset<Row> transform(Dataset<Row> df,String bucketFileName, JavaSparkContext jsc) {

        registerUdfs();

        String geomCol = findGeometryColumn(df);

        long n1 = df.count();
        System.out.println("Row count n1 = " + n1);

        Dataset<Row> transformed = df
                .withColumn("geometry",
                        callUDF("stringOrGeomToGeometry", col(geomCol)))
                .filter(col("geometry").isNotNull());

        long n2 = transformed.count();
        System.out.println("Row count n2 = " + n2);

        transformed=addGeohash(transformed);

        transformed = transformed.filter(
                col("geometry.geohash_numeric").isNotNull()
        );

        long n3 = transformed.count();
        System.out.println("Row count n3 = " + n3);

        int[] BucketBorderForNewRecords = BucketManager2.computeBucketBorders(transformed, bucketFileName);

        Broadcast<int[]> broadcastBorders = jsc.broadcast(BucketBorderForNewRecords);

        SparkUdfs.registerFindFloorAndCeilingUdf(spark, broadcastBorders);

        transformed = transformed.withColumn(
                "geometry",
                col("geometry").withField(
                        "partition_number",
                        callUDF("findFloorAndCeiling", col("geometry.geohash_numeric"))
                )
        );



        /*transformed = transformed
                .withColumn("floor_val", col("bounds.floor"))
                .withColumn("ceiling_val", col("bounds.ceiling"))
                .drop("bounds");
                */
        return transformed;
    }



    // ---------------------------
    // Silver
    // ---------------------------

    private void writeSilver(TableSpec silver, JavaSparkContext jsc, Dataset<Row> df) throws NoSuchTableException {

        String fullName = silver.database() + "." + silver.table();
        boolean exists = spark.catalog().tableExists(fullName);

        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        Dataset<Row> transformed = transform(df, bucketFileName, jsc );

        transformed.printSchema();

        if (!exists) {
            System.out.println("Now creating silver table with ID column...");
            IcebergTableCreatorWithPartitioning.createIcebergTableFromSchema(
                    spark,
                    transformed.schema(),
                    silver.database(),
                    silver.table(),
            List.of("identity(geometry.partition_number.floor)", "identity(geometry.partition_number.ceiling)")
            );
        } else {

            StructType tableSchema = spark.table(fullName).schema();


            if (!tableSchema.sameType(transformed.schema())) {
                throw new RuntimeException(
                        "Schema mismatch for table: " + fullName
                );
            }
        }

        transformed.writeTo(fullName).append();
        System.out.println("Data appended to silver layer");
    }

    private void updateBucket(TableSpec silver) {

        String fullName = silver.database() + "." + silver.table();
        String metadataTable = fullName + ".partitions";
        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        File f = new File(bucketFileName);
        if (!f.exists()) {
            System.out.println("Bucket " + bucketFileName + " does not exist to update");
            return;
        }



        Dataset<Row> stats = spark.read()
                .table(metadataTable)
                .filter(col("partition").isNotNull())
                .select(
                        col("partition").getField("geometry.partition_number.floor").as("floor_val"),
                        col("partition").getField("geometry.partition_number.ceiling").as("ceiling_val"),
                        col("record_count")
                )
                .filter(col("floor_val").isNotNull().and(col("ceiling_val").isNotNull()))
                .groupBy("floor_val", "ceiling_val")
                .agg(sum(col("record_count")).as("total_count"));

        List<Row> partitionStats = stats.collectAsList();

        BucketManager2.Bucket bucket = BucketManager2.loadBucket(bucketFileName);

        System.out.println("partition metadata"+partitionStats);


        BucketManager2.updateTreeFromStats(partitionStats, bucket);

        System.out.println("Bucket updated to " + bucketFileName);
    }



    // ---------------------------
    // Helpers
    // ---------------------------

    private void registerUdfs() {
        UDFRegistry.registerAll(spark, adapter);
        CoordinateToGeohashNumericUdfRegistry.registerAll(spark);
    }

    private String findGeometryColumn(Dataset<Row> df) {

        return Arrays.stream(df.columns())
                .filter(c -> c.equalsIgnoreCase("geometry"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No geometry column found"));
    }

    private Dataset<Row> addGeohash(Dataset<Row> df) {

        Column fromCenter = callUDF("CoordinateToGeohashNumeric",
                col("geometry.center.x"), col("geometry.center.y"));

        Column fromFirst = callUDF("CoordinateToGeohashNumeric",
                col("geometry.parts").getItem(0).getField("coordinates").getItem(0).getField("x"),
                col("geometry.parts").getItem(0).getField("coordinates").getItem(0).getField("y"));

        Column centerOk = col("geometry.center.x").isNotNull().and(col("geometry.center.y").isNotNull());

        Column firstOk =
                size(col("geometry.parts")).gt(0)
                        .and(size(col("geometry.parts").getItem(0).getField("coordinates")).gt(0));

        Column geohash = when(centerOk, fromCenter)
                .when(firstOk, fromFirst)
                .otherwise(lit(null));

        return df.withColumn("geometry",
                col("geometry").withField("geohash_numeric", geohash));
    }
}










