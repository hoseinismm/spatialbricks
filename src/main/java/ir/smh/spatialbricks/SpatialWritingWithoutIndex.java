package ir.smh.spatialbricks;
import ir.smh.spatialbricks.createsql.IcebergTableCreator;
import ir.smh.spatialbricks.createsql.IcebergTableCreatorWithPartitioning;
import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.GeohashToIntegerUdfRegistry;
import ir.smh.spatialbricks.encoder.udf.SparkUdfs;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.sedona.core.formatMapper.GeoJsonReader;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.*;

public class SpatialWritingWithoutIndex implements Serializable {

    private final SparkSession spark;
    private final GeometryOptions options;
    private final GeometryReader<?> adapter;



    public SpatialWritingWithoutIndex(SparkSession spark, GeometryOptions options, GeometryReader<?> adapter) {
        this.spark = spark;
        this.options = options;
        this.adapter = adapter;



    }

    public void processFile(TableSpec bronze, TableSpec silver, String inputPath)
            throws NoSuchTableException, IOException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = readInput(inputPath, jsc);

        writeBronze(bronze, df);

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

    private Dataset<Row> transform(Dataset<Row> df) {

        registerUdfs();

        String geomCol = findGeometryColumn(df);

        Dataset<Row> transformed = df
                .withColumn("geometry",
                        callUDF("stringOrGeomToGeometry", df.col(geomCol)))
                .filter(col("geometry").isNotNull());

        transformed = addGeohash(transformed);

        transformed = transformed.filter(
                col("geohash_numeric").isNotNull()
        );

        return transformed.withColumn("partitionnumber", lit(-1).cast("int"));
    }



    // ---------------------------
    // Silver
    // ---------------------------

    private void writeSilver(TableSpec silver, JavaSparkContext jsc, Dataset<Row> df) throws NoSuchTableException {

        String fullName = silver.database() + "." + silver.table();
        boolean exists = spark.catalog().tableExists(fullName);

        Dataset<Row> transformed = transform(df);



        if (!exists) {
            System.out.println("Now creating silver table with ID column...");

            transformed = transformed.withColumn("id", monotonically_increasing_id());


            IcebergTableCreatorWithPartitioning.createIcebergTableFromSchema(
                    spark,
                    transformed.schema(),
                    silver.database(),
                    silver.table(),
                    List.of("identity(partitionnumber)")
            );
        } else {

            long maxId = spark.read().format("iceberg").load(fullName)
                    .agg(max("id").as("my_max"))
                    .first()
                    .getAs("my_max");

            transformed = transformed.withColumn("id", monotonically_increasing_id().plus(maxId + 1));

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


    // ---------------------------
    // Helpers
    // ---------------------------

    private void registerUdfs() {
        UDFRegistry.registerAll(spark, adapter);
        GeohashToIntegerUdfRegistry.registerAll(spark);
    }

    private String findGeometryColumn(Dataset<Row> df) {

        return Arrays.stream(df.columns())
                .filter(c -> c.equalsIgnoreCase("geometry"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No geometry column found"));
    }

    private Dataset<Row> addGeohash(Dataset<Row> df) {

        return df.withColumn(
                "geohash_numeric",
                callUDF("geohashToInteger", col("geometry"))
        );
    }
}










