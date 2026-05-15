package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.SparkUdfs;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.stream.Collectors;


public class AddIndexToSpatialData implements Serializable {
    private final SparkSession spark;
    private final GeometryOptions options;
    private final GeometryReader<?> adapter;

    public AddIndexToSpatialData(SparkSession spark, GeometryOptions options, GeometryReader<?> adapter) {
        this.spark = spark;
        this.options = options;
        this.adapter = adapter;
    }

    public void AddorUpdateIndex(TableSpec silver) throws NoSuchTableException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        String fullName = silver.database() + "." + silver.table();

        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        Dataset<Row> table =
                spark.read()
                        .format("iceberg")
                        .load(fullName).cache();
        spark.sql("""          
                SELECT count(*) AS silvercount1
                FROM spark_catalog.silverlayer.FireStations
         """).show();


        Dataset<Row> notIndexedRows = table.filter(col("bucket_min").equalTo(-1));
        Dataset<Row> indexedRows = table.filter(col("bucket_min").notEqual(-1));


        collectionOfBordersAndNewBorders collector = computeBucketBorders(notIndexedRows, bucketFileName);



        Integer[] oldBordersForOldRecords =
                Arrays.stream(collector.getoldBordersForOldRecords()).boxed().toArray(Integer[]::new);

        Dataset<Row> previousIndexedRowsAffectedFromNewIndexing =
                indexedRows.filter(
                        functions.col("bucket_min").isin((Object[]) oldBordersForOldRecords)
                );
        List<Row> plist = previousIndexedRowsAffectedFromNewIndexing
                .select("bucket_min").distinct().collectAsList();

        if (!plist.isEmpty()) {
        String partList = plist.stream()
                .map(r -> String.valueOf(r.getInt(0)))   // این درست است
                .collect(Collectors.joining(","));

        spark.sql(String.format("""
    DELETE FROM spark_catalog.silverlayer.FireStations WHERE bucket_min IN (%s)
""", partList));

        } else {
            System.out.println("No partitions to delete");
        }

        Broadcast<int[]> broadcastBorders = jsc.broadcast(collector.getupdatesForOldRecords());

        SparkUdfs.registerFindFloorUdf(spark, broadcastBorders);

        previousIndexedRowsAffectedFromNewIndexing =  previousIndexedRowsAffectedFromNewIndexing.withColumn(
                "bucket_min",
                callUDF("findFloor", col("geohash_numeric"))
        );


        previousIndexedRowsAffectedFromNewIndexing.writeTo(fullName)
                .overwritePartitions();

        spark.sql("""          
                SELECT count(*) AS silvercount2
                FROM spark_catalog.silverlayer.FireStations
               
         """).show();

        int[] newNeededBucketMinsForNewRecords= collector.getneededBucketMinsForNewRecords();

        Broadcast<int[]> broadcastBorders2 = jsc.broadcast(newNeededBucketMinsForNewRecords);

        SparkUdfs.registerFindFloorUdf(spark, broadcastBorders2);

        notIndexedRows = notIndexedRows.withColumn(
                "bucket_min",
                callUDF("findFloor", col("geohash_numeric"))
        );

          spark.sql("""
                  DELETE FROM %s WHERE bucket_min = -1
                  """.formatted(fullName));

        spark.sql("""          
                SELECT count(*) AS silvercount3
                FROM spark_catalog.silverlayer.FireStations
                
         """).show();

        notIndexedRows.writeTo(fullName).append();

        spark.sql("""          
                SELECT count(*) AS silvercount4
                FROM spark_catalog.silverlayer.FireStations
                
         """).show();

        long count = spark.sql("""
SELECT count(*) AS cnt
FROM silverlayer.FireStations
WHERE bucket_min = -1
""").first().getLong(0);

        System.out.println("count = " + count);
        System.out.println("notIndexedRows count = " + notIndexedRows.count());
        System.out.println("indexedRows count = " + indexedRows.count());
        System.out.println("previousIndexedRowsAffectedFromNewIndexing count = "
                + previousIndexedRowsAffectedFromNewIndexing.count());
        System.out.println("oldBordersForOldRecords = " + oldBordersForOldRecords.length);
        System.out.println("newNeededBucketMinsForNewRecords = " + newNeededBucketMinsForNewRecords.length);
        System.out.println("broadcastBorders = " + broadcastBorders);
        System.out.println("broadcastBorders2 = " + broadcastBorders2);
        System.out.println("oldBordersForOldRecords = " + oldBordersForOldRecords.length);





        return;
    }

    private collectionOfBordersAndNewBorders computeBucketBorders(Dataset<Row> df, String bucketFile) { // اضافه کردن ورودی نام فایل
        List<Integer> list = df
                .select("geohash_numeric")
                .as(Encoders.INT())
                .collectAsList();

        int[] geos = list.stream().mapToInt(Integer::intValue).toArray();
        BucketManager.Bucket bucket;

        File f = new File(bucketFile);

        if (f.exists()) {
            bucket = BucketManager.loadBucket(bucketFile);
            System.out.println("Bucket loaded from: " + bucketFile);
            if (bucket == null) bucket = BucketManager.initialBucket();
        } else {
            bucket = BucketManager.initialBucket();
            System.out.println("Created new bucket for: " + bucketFile);
        }

        BucketManager.BucketCollection result = BucketManager.addGeosToBuckets(geos, bucket, 2048);
        bucket=result.getBucket();
        int[] oldBordersForOldRecords=result.getoldBordersForOldRecords().stream().mapToInt(Integer::intValue).toArray();
        int[] updatesForOldRecords=result.getupdatesForOldRecords().stream().mapToInt(Integer::intValue).toArray();
        int[] neededBucketMinsForNewRecords=result.getneededBucketMinsForNewRecords().stream().mapToInt(Integer::intValue).toArray();

        BucketManager.saveBucket(bucket, bucketFile);
        Arrays.sort(oldBordersForOldRecords);
        Arrays.sort(updatesForOldRecords);
        Arrays.sort(neededBucketMinsForNewRecords);


        return new collectionOfBordersAndNewBorders(oldBordersForOldRecords,updatesForOldRecords,neededBucketMinsForNewRecords);
    }

    private static class collectionOfBordersAndNewBorders {
        final private int[] oldBordersForOldRecords;
        final private int[] updatesForOldRecords;
        final private int[] neededBucketMinsForNewRecords;


        public collectionOfBordersAndNewBorders(int[] oldBordersForOldRecords,int[] updatesForOldRecords,int[] neededBucketMinsForNewRecord) {
            this.oldBordersForOldRecords = oldBordersForOldRecords;
            this.updatesForOldRecords = updatesForOldRecords;
            this.neededBucketMinsForNewRecords = neededBucketMinsForNewRecord;
        }
        public int[] getoldBordersForOldRecords() {
            return oldBordersForOldRecords;
        }
        public int[] getupdatesForOldRecords() {
            return updatesForOldRecords;
        }
        public int[] getneededBucketMinsForNewRecords() {
            return neededBucketMinsForNewRecords;
        }


    }
}
