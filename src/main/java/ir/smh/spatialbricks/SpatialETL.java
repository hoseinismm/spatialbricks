package ir.smh.spatialbricks;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;

import ir.smh.spatialbricks.encoder.udf.GeohashToIntegerUdfRegistry;
import ir.smh.spatialbricks.encoder.udf.MinInBucketUdfRegistry;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import ir.smh.spatialbricks.createsql.IcebergTableCreator;
import ir.smh.spatialbricks.createsql.IcebergTableCreatorWithPartitioning;
import org.apache.sedona.core.formatMapper.GeoJsonReader;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class SpatialETL implements Serializable {

    private final SparkSession spark;
    private final GeometryOptions options;
    private final GeometryReader<?> adapter;
    private List<Double> splitList;

    public SpatialETL(SparkSession spark, GeometryOptions options, GeometryReader<?> adapter) {
        this.spark = spark;
        this.options = options;
        this.adapter = adapter;
    }

    public void processFile(TableSpec bronze, TableSpec silver, String inputPath) throws NoSuchTableException, IOException {


        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> df = null;

        // ایجاد جدول برنزی

        if ((inputPath.toLowerCase().endsWith(".json"))||inputPath.toLowerCase().endsWith(".geojson")) {
            df = Adapter.toDf(GeoJsonReader.readToGeometryRDD(jsc, inputPath), spark);
            Dataset<Row> dfGeoJson = df.withColumn(
                    "geometry",
                    expr("ST_AsGeoJSON(geometry)")
            );
            IcebergTableCreator.createIcebergTableFromSchema(spark, dfGeoJson.schema(), bronze.database(), bronze.table());
            dfGeoJson.writeTo(bronze.database() + "." + bronze.table()).append();

        } else if (inputPath.toLowerCase().endsWith(".parquet")) {
            df = spark.read().parquet(inputPath);
            IcebergTableCreator.createIcebergTableFromSchema(spark, df.schema(), bronze.database(), bronze.table());
            df.writeTo(bronze.database() + "." + bronze.table()).append();

        } else if (inputPath.toLowerCase().endsWith(".csv")) {
            df = spark.read().csv(inputPath);
            df.writeTo(bronze.database() + "." + bronze.table()).append();

        } else {
            throw new IllegalArgumentException("Unsupported file format: " + inputPath);
        }

        // ثبت UDF و تبدیل
        UDFRegistry.registerAll(spark, options, adapter);

        // پیدا کردن نام واقعی ستون با ignore case، اعتبارسنجی وجود ستون geometry
        String geomCol = Arrays.stream(df.columns())
                .filter(c -> c.equalsIgnoreCase("geometry"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No geometry column found"));

        Dataset<Row> transformed = df.withColumn(
                "geometry",
                callUDF("stringOrGeomToGeometry", df.col(geomCol))
        );


        transformed = transformed.filter(col("geometry").isNotNull());

        //long total = transformed.count();


        GeohashToIntegerUdfRegistry.registerAll(spark);

        MinInBucketUdfRegistry.registerAll(spark);

        transformed =transformed.withColumn(
                "geohash_numeric",
                functions.callUDF(
                        "geohashToLong",
                        transformed.col("geometry")
                )
        );



        // تعداد کل رکوردها
        long totalCount = transformed.count();

        // اندازه تقریبی هر بازه
        long bucketSize = 64;
        long numBuckets = (totalCount + bucketSize - 1) / bucketSize; // سقف تقسیم

        System.out.println("Total count: " + totalCount + ", Number of buckets: " + numBuckets);

        // درصدهای مورد نیاز برای approxQuantile
        double[] probabilities = new double[(int) numBuckets + 1];
        for (int i = 0; i <= numBuckets; i++) {
            probabilities[i] = i * 1.0 / numBuckets;
        }

        // محاسبه تقریبی مرزهای بازه‌ها
        double[] splits = transformed.stat().approxQuantile("geohash_numeric", probabilities, 0.001);



        List<Double> splitList = Arrays.stream(splits)
                .boxed()
                .collect(Collectors.toList());

        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(new File("splits.json"), splitList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        MinInBucketUdfRegistry.setSplitList(splitList);


        System.out.println("Splits: " + Arrays.toString(splits));

        transformed = transformed.withColumn(
                "bucket_min",
                functions.callUDF(
                        "minInBucketUDF",
                        functions.col("geohash_numeric")
                )
        );


        // ایجاد جدول نقره‌ای
        IcebergTableCreatorWithPartitioning.createIcebergTableFromSchema(
                spark,
                transformed.schema(),
                silver.database(),
                silver.table(),
                List.of("identity(bucket_min)")
        );

        transformed.writeTo(silver.database() + "." + silver.table()).append();
        //spark.sql(String.format("ALTER TABLE %s.%s WRITE ORDERED BY (geometry.center.x ASC)", silver.database(), silver.table()
        //));

        //Dataset<Row> df2 = transformed.withColumn("center_x", col("geometry.center.x"));



        //Dataset<Row> df2 = transformed.withColumn("center_x", col("geometry.center.x"));






        // حالا dfWithBucket شامل ستون bucket_min است
        transformed.show();
    }
}



