package ir.smh.spatialbricks;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import ir.smh.spatialbricks.createsql.IcebergTableCreator;
import ir.smh.spatialbricks.createsql.IcebergTableCreatorWithPartitioning;
import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.GeohashToIntegerUdfRegistry;
import ir.smh.spatialbricks.encoder.udf.MinInBucketUdfRegistry;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;
import org.apache.sedona.core.formatMapper.GeoJsonReader;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import shaded.parquet.it.unimi.dsi.fastutil.longs.LongArrayList;

import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.*;

import static org.apache.spark.sql.functions.*;

public class SpatialETL2 implements Serializable {

    private final SparkSession spark;
    private final GeometryOptions options;
    private final GeometryReader<?> adapter;
    private List<Double> splitList;

    public SpatialETL2(SparkSession spark, GeometryOptions options, GeometryReader<?> adapter) {
        this.spark = spark;
        this.options = options;
        this.adapter = adapter;
    }

    private static void insertSorted(List<Row> list, Row newRow) {

        long value = newRow.getLong(0);
        int pos = 0;

        while (pos < list.size() && list.get(pos).getLong(0) < value) {
            pos++;
        }

        list.add(pos, newRow);
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
        UDFRegistry.registerAll(spark, adapter);

        // پیدا کردن نام واقعی ستون با ignore case
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



        transformed =transformed.withColumn(
                "geohash_numeric",
                functions.callUDF(
                        "geohashToLong",
                        transformed.col("geometry")
                )
        );

        transformed = transformed.withColumn("geohashmin", lit(0L));

        transformed.persist(StorageLevel.MEMORY_AND_DISK());
        transformed.count();


        long GLOBAL_MAX = (long)Math.pow(32, 6) ;

        Dataset<Row> metaDf = transformed
                .groupBy(col("geohashmin"))
                .count()
                .withColumnRenamed("count", "record_count");

// bucketهایی که باید split شوند
        List<Row> oversized = new ArrayList<>(
                metaDf
                        .filter(col("record_count").gt(64))
                        .orderBy("geohashmin")
                        .collectAsList()
        );


        for (int i = 0; i < oversized.size(); i++) {

            Row bucket = oversized.get(i);

            long min = bucket.getLong(0);   // bucket_min
            long count = bucket.getLong(1);

            long max;

            if (i == oversized.size() - 1) {
                max = GLOBAL_MAX;
            } else {
                long nextMin = oversized.get(i + 1).getLong(0);
                max = nextMin;
            }

            if ((max-min)>1) {
                long mid = min + (max - min) / 2;


                // split کردن bucket
                transformed = transformed.withColumn(
                        "geohashmin",
                        when(
                                col("geohashmin").equalTo(min)
                                        .and(col("geohash_numeric").geq(mid)),
                                lit(mid)
                        ).otherwise(col("geohashmin"))
                );

                // حذف bucket فعلی
                oversized.remove(i);
                i--; // چون عنصر حذف شد index یک خانه عقب می‌آید

// شمارش دو bucket جدید
                long minCount = transformed
                        .filter(col("geohashmin").equalTo(min))
                        .count();

                long midCount = transformed
                        .filter(col("geohashmin").equalTo(mid))
                        .count();


// اگر bucket چپ هنوز بزرگ است
                if (minCount > 64) {
                    Row newRow = RowFactory.create(min, minCount);
                    insertSorted(oversized, newRow);
                }

// اگر bucket راست هنوز بزرگ است
                if (midCount > 64) {
                    Row newRow = RowFactory.create(mid, midCount);
                    insertSorted(oversized, newRow);
                }
            }   else continue;
        }









        IcebergTableCreatorWithPartitioning.createIcebergTableFromSchema(
                spark,
                transformed.schema(),
                silver.database(),
                silver.table(),
                List.of("identity(geohashmin)")
        );




        transformed.writeTo(silver.database() + "." + silver.table()).append();








        transformed.show();
    }
    // ---------------------------------------------
// ساخت bucketها به صورت pure Java (بدون Spark)
// ---------------------------------------------
    private static Bucket addGeosToBuckets(long[] newgeos, Bucket buckets, long MAX_SIZE) {
        try {
            for (long geo : newgeos) {
                try {
                    Bucket current = buckets;
                    while (current.isLeaf) {
                        if (geo >= current.mid) {
                            current = current.right;
                        } else {
                            current = current.left;
                        }
                    }
                    current.count++;
                    current.geos.add(geo);

                    while ((current.count > MAX_SIZE) && (current.max - current.min >= 2L)) {
                        current.left = new Bucket(current.min, current.mid, 0, current);
                        current.right = new Bucket(current.mid, current.max, 0, current);
                        current.isLeaf=true;
                        current.left.brother = current.right;
                        current.right.brother = current.left;

                        for (int j = 0; j < current.geos.size(); j++) {
                            long g = current.geos.getLong(j);
                            if (g >= current.mid) {
                                current.right.geos.add(g);
                                current.right.count++;
                            } else {
                                current.left.geos.add(g);
                                current.left.count++;
                            }
                        }

                        current.geos = null;
                        current.count = 0;

                        if (current.left.count > MAX_SIZE) {
                            current = current.left;
                        } else if (current.right.count > MAX_SIZE) {
                            current = current.right;
                        } else break;
                    }
                } catch (Exception e) {
                    System.err.println("Error processing this geo value: " + geo + " - " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error in addGeosToBuckets: " + e.getMessage());
            e.printStackTrace();
        }
        return buckets;
    }


    private static Bucket removeGeosّFromBuckets(long[] removedgeos, Bucket buckets, long MIN_SIZE) {
        try {
            for (long geo : removedgeos) {
                try {
                    Bucket current = buckets;
                    while (current.right != null) {
                        if (geo >= current.mid) {
                            current = current.right;
                        } else {
                            current = current.left;
                        }
                    }

                    if (current.geos != null && current.geos.rem(geo)) {
                        current.count--;
                    }

                    while (current.parent != null) {
                        if (current.count + current.brother.count < MIN_SIZE) {
                            LongArrayList temp = new LongArrayList(current.geos.size() + current.brother.geos.size());
                            temp.addAll(current.geos);
                            temp.addAll(current.brother.geos);
                            current.parent.geos = temp;

                            current.parent.count = current.count + current.brother.count;
                            current = current.parent;
                            current.isLeaf = false;

                            current.left = null;
                            current.right = null;

                        } else break;
                    }
                } catch (Exception e) {
                    System.err.println("Error processing this geo value for removal: " + geo + " - " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error in removeGeosToBuckets: " + e.getMessage());
            e.printStackTrace();
        }
        return buckets;
    }

    private static class Bucket {
        long min;
        long max;
        long count;
        long mid;
        Bucket brother;
        Bucket parent;
        Bucket right;
        Bucket left;
        LongArrayList geos;
        boolean isLeaf;

        Bucket(long min, long max, long count, LongArrayList geos, Bucket parent) {
            this.parent = parent;
            this.min = min;
            this.max = max;
            this.count = count;
            this.geos = geos;
            this.right = null;
            this.left = null;
            this.mid = min + (max - min) / 2;
            this.isLeaf = false;
        }

        Bucket(long min, long max, long count, Bucket parent) {
            this.parent = parent;
            this.min = min;
            this.max = max;
            this.count = count;
            this.geos = new LongArrayList();
            this.right = null;
            this.left = null;
            this.mid = min + (max - min) / 2; // رفع باگ: این خط جا افتاده بود
            this.isLeaf = false;
        }


    }

}









