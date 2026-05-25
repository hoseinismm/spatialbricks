package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.GeometryOptions;
import ir.smh.spatialbricks.encoder.GeometryReader;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Encoder;
import static org.apache.spark.sql.functions.*;

import java.io.File;
import java.io.Serializable;
import java.util.*;


public class AddIndexToSpatialData2 implements Serializable {

    private final SparkSession spark;

    public AddIndexToSpatialData2(SparkSession spark) {
        this.spark = spark;
    }

    public void AddorUpdateIndex(TableSpec silver) throws NoSuchTableException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        String fullName = silver.database() + "." + silver.table();
        String bucketFileName = "bucket" + silver.database() + "_" + silver.table() + ".gz";

        Dataset<Row> table =
                spark.read()
                        .format("iceberg")
                        .load(fullName);

        Dataset<Row> notIndexedRows = table.filter(col("bucket_min").equalTo(-1));
        Dataset<Row> indexedRows = table.filter(col("bucket_min").notEqual(-1));

        collectionOfBordersAndNewBorders collector =
              computeBucketBorders(notIndexedRows, bucketFileName);

        int[] oldBorders = collector.getoldBordersForOldRecords();
        int[] updatesForOld = collector.getupdatesForOldRecords();
        int[] newBorders = collector.getneededBucketMinsForNewRecords();

        Broadcast<int[]> broadcastUpdates = jsc.broadcast(updatesForOld);
        Broadcast<int[]> broadcastNew = jsc.broadcast(newBorders);

        Dataset<Row> affectedIndexedRows = indexedRows.limit(0);

        if (oldBorders.length > 0) {
            affectedIndexedRows = indexedRows.filter(col("bucket_min").isin(
                    Arrays.stream(oldBorders).boxed().toArray()
            ));
        }

        Dataset<Row> recomputedOld =
                recomputeBucketsLight(affectedIndexedRows, broadcastUpdates);

        Dataset<Row> recomputedNew =
                recomputeBucketsLight(notIndexedRows, broadcastNew);

        Dataset<Row> updates =
                recomputedOld.unionByName(recomputedNew);

        // register temp view for merge
        updates.createOrReplaceTempView("updates_view");

        String mergeSql =
                "MERGE INTO " + fullName + " t " +
                        "USING updates_view u " +
                        "ON t.id = u.id " +
                        "WHEN MATCHED THEN UPDATE SET bucket_min = u.bucket_new";

        spark.sql(mergeSql);

        System.out.println("Reindexed rows = " + recomputedOld.count());
        System.out.println("New indexed rows = " + recomputedNew.count());
    }


    private Dataset<Row> recomputeBucketsLight(
            Dataset<Row> dataset,
            Broadcast<int[]> bordersBroadcast
    ) {

        Dataset<Row> pruned =
                dataset.select("id", "geohash_numeric");

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("bucket_new", DataTypes.IntegerType);

        Encoder<Row> encoder = Encoders.row(schema);

        MapPartitionsFunction<Row, Row> func = iterator -> {

            int[] borders = bordersBroadcast.value();
            List<Row> out = new ArrayList<>();

            while (iterator.hasNext()) {

                Row r = iterator.next();

                int id = r.getInt(0);
                int geo = r.getInt(1);

                int bucket = findFloor(geo, borders);

                out.add(RowFactory.create(id, bucket));
            }

            return out.iterator();
        };

        return pruned.mapPartitions(func, encoder);
    }

    private Dataset<Row> applyUpdates(
            Dataset<Row> table,
            Dataset<Row> updates
    ) {

        Dataset<Row> joined =
                table.join(updates,
                        table.col("id").equalTo(updates.col("id")),
                        "left");

        return joined.withColumn(
                "bucket_min",
                functions.coalesce(col("bucket_new"), col("bucket_min"))
        ).drop("bucket_new");
    }



    private static int findFloor(int value, int[] borders) {

        int lo = 0;

        int hi = borders.length - 1;

        while (lo <= hi) {

            int mid = (lo + hi) >>> 1;

            if (borders[mid] <= value) {

                lo = mid + 1;

            } else {

                hi = mid - 1;

            }

        }

        if (hi < 0) {

            return -1;

        }

        return borders[hi];

    }

    private collectionOfBordersAndNewBorders computeBucketBorders(Dataset<Row> df, String bucketFile) {

        List<Integer> list =

                df.select("geohash_numeric")

                        .as(Encoders.INT())

                        .collectAsList();

        int[] geos = list.stream().mapToInt(Integer::intValue).toArray();

        BucketManager.Bucket bucket;

        File f = new File(bucketFile);

        if (f.exists()) {

            bucket = BucketManager.loadBucket(bucketFile);

            if (bucket == null) bucket = BucketManager.initialBucket();

        } else {

            bucket = BucketManager.initialBucket();

        }

        BucketManager.BucketCollection result =

                BucketManager.addGeosToBuckets(geos, bucket, 512);

        bucket = result.getBucket();

        int[] oldBorders =

                result.getoldBordersForOldRecords().stream().mapToInt(Integer::intValue).toArray();

        int[] updates =

                result.getupdatesForOldRecords().stream().mapToInt(Integer::intValue).toArray();

        int[] needed =

                result.getneededBucketMinsForNewRecords().stream().mapToInt(Integer::intValue).toArray();

        BucketManager.saveBucket(bucket, bucketFile);

        Arrays.sort(oldBorders);

        Arrays.sort(updates);

        Arrays.sort(needed);

        return new collectionOfBordersAndNewBorders(oldBorders, updates, needed);

    }

    private static class collectionOfBordersAndNewBorders {

        private final int[] oldBordersForOldRecords;

        private final int[] updatesForOldRecords;

        private final int[] neededBucketMinsForNewRecords;

        public collectionOfBordersAndNewBorders(

                int[] oldBordersForOldRecords,

                int[] updatesForOldRecords,

                int[] neededBucketMinsForNewRecords) {

            this.oldBordersForOldRecords = oldBordersForOldRecords;

            this.updatesForOldRecords = updatesForOldRecords;

            this.neededBucketMinsForNewRecords = neededBucketMinsForNewRecords;

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
