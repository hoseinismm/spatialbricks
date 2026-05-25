package ir.smh.spatialbricks;

import ir.smh.spatialbricks.createsql.IcebergTableCreatorWithPartitioning;
import ir.smh.spatialbricks.encoder.GeometryResult;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;


public class AddindexToSpatialData4 implements Serializable {

    private final SparkSession spark;

    public AddindexToSpatialData4(SparkSession spark) {
        this.spark = spark;
    }

    public void AddOrUpdateIndex(TableSpec silver, String xColumnName, String yColumnName)
            throws NoSuchTableException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        String mainTableName = silver.database() + "." + silver.table();

        String bucketFileName = "bucket" + silver.database() + "_" + silver.table() + ".gz";

        // -----------------------------
        // Load main table
        // -----------------------------
        Dataset<Row> mainTable = spark.read()
                .format("iceberg")
                .load(mainTableName);

        Dataset<Row> notIndexedRows = mainTable.filter(col("partitionnumber").equalTo(-1));

        if (!newIndexEntries.isEmpty()) {
            notIndexedRows =
                    computeIndexForNewRows(newIndexEntries, xColumnName, yColumnName);
                    System.out.println("notindexedrows: " + notIndexedRows.count());
        }
        else {
            System.out.println("No data or all of data are in indexed partitions!");
            return;
        }

        Dataset<Row> indexedRows  = spark.emptyDataFrame();

        if (spark.catalog().tableExists(indexTableName)) {

            indexedRows = spark.read()
                    .format("iceberg")
                    .load(indexTableName);

        } else {
            StructType indexSchema = new StructType()
                    .add("id", DataTypes.LongType)
                    .add("geohash_numeric", DataTypes.IntegerType)
                    .add("bucket_min", DataTypes.IntegerType);

            IcebergTableCreatorWithPartitioning.createIcebergTableFromSchema(
                    spark,
                    indexSchema,
                    silver.database(),
                    silver.table()+"_index",
                    List.of("identity(bucket_min)")
            );

            indexedRows = spark.createDataFrame(
                    new ArrayList<Row>(),
                    indexSchema
            );
        }

        // -----------------------------
        // Compute bucket borders using index table
        // -----------------------------
        collectionOfBordersAndNewBorders collector =
                computeBucketBorders(notIndexedRows, bucketFileName);

        int[] oldBorders = collector.getoldBordersForOldRecords();
        int[] updatesForOld = collector.getupdatesForOldRecords();
        int[] newBorders = collector.getneededBucketMinsForNewRecords();

        // -----------------------------
        // Prepare empty datasets with fixed schema
        // -----------------------------

        StructType schema = new StructType()
                .add("id", DataTypes.LongType)
                .add("geohash_numeric", DataTypes.IntegerType)
                .add("bucket_new", DataTypes.IntegerType);

        Dataset<Row> recomputedOld = spark.createDataFrame(new ArrayList<Row>(), schema);
        Dataset<Row> recomputedNew = spark.createDataFrame(new ArrayList<Row>(), schema);

        Broadcast<int[]> broadcastUpdates = jsc.broadcast(updatesForOld);
        Broadcast<int[]> broadcastNew = jsc.broadcast(newBorders);

        try {

            // -----------------------------
            // Recompute affected old indexed rows
            // -----------------------------
            if (oldBorders != null && oldBorders.length > 0) {
                Object[] oldBorderObjects = Arrays.stream(oldBorders).boxed().toArray();

                Dataset<Row> affectedIndexedRows = indexedRows.filter(
                        col("bucket_min").isin(oldBorderObjects)
                );

                recomputedOld = recomputeBucketsLight(affectedIndexedRows, broadcastUpdates);
            }

            // -----------------------------
            // 7) Recompute new rows
            // -----------------------------
            if (!notIndexedRows.isEmpty()) {
                recomputedNew = recomputeBucketsLight(notIndexedRows, broadcastNew);
            }

            // -----------------------------
            // 8) Combine all updates
            // -----------------------------
            Dataset<Row> updates = recomputedOld.unionByName(recomputedNew);

            if (updates.isEmpty()) {
                System.out.println("No updates needed.");
                System.out.println("Reindexed rows = 0");
                System.out.println("New indexed rows = 0");
                return;
            }

            // -----------------------------
            // Update index table
            // -----------------------------
            updates.createOrReplaceTempView("updates_view");



            updates.groupBy("id")
                    .count()
                    .filter("count > 1")
                    .show();

            indexedRows.groupBy("id")
                    .count()
                    .filter("count > 1")
                    .show();
            mainTable.groupBy("id")
                    .count()
                    .filter("count > 1")
                    .show();




            spark.sql(
                    "MERGE INTO " + indexTableName + " t " +
                            "USING updates_view u " +
                            "ON t.id = u.id " +
                            "WHEN MATCHED THEN UPDATE SET t.bucket_min = u.bucket_new " +
                            "WHEN NOT MATCHED THEN INSERT (id, geohash_numeric, bucket_min) VALUES (u.id, u.geohash_numeric, u.bucket_new)"

            );

            spark.sql("MERGE INTO " + mainTableName  + " t USING updates_view u ON t.id = u.id WHEN MATCHED THEN UPDATE SET t.partitionnumber = u.bucket_new");





            System.out.println("Index table updated successfully.");
            System.out.println("Main table synced successfully.");
            System.out.println("Reindexed rows = " + recomputedOld.count());
            System.out.println("New indexed rows = " + recomputedNew.count());

        } finally {
            broadcastUpdates.unpersist();
            broadcastNew.unpersist();
        }
    }
    private Dataset<Row> recomputeBucketsLight(
            Dataset<Row> dataset,
            Broadcast<int[]> bordersBroadcast
    ) {

        Dataset<Row> pruned = dataset.select("id", "geohash_numeric");


        int idIdx = pruned.schema().fieldIndex("id");
        int geoIdx = pruned.schema().fieldIndex("geohash_numeric");


        StructType outputSchema = new StructType()
                .add("id", DataTypes.LongType)
                .add("geohash_numeric", DataTypes.IntegerType)
                .add("bucket_new", DataTypes.IntegerType);

        Encoder<Row> encoder = Encoders.row(outputSchema);

        MapPartitionsFunction<Row, Row> func = iterator -> {
            int[] borders = bordersBroadcast.value();
            List<Row> out = new ArrayList<>();

            while (iterator.hasNext()) {
                Row r = iterator.next();


                if (r.isNullAt(idIdx) || r.isNullAt(geoIdx)) {
                    continue;
                }

                long id = r.getLong(idIdx);
                int geo = r.getInt(geoIdx);

                int bucket = findFloor(geo, borders);

                // خروجی شامل هر ۳ ستون است
                out.add(RowFactory.create(id, geo, bucket));
            }
            return out.iterator();
        };

        return pruned.mapPartitions(func, encoder);
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

    private Dataset<Row> computeIndexForNewRows(Dataset<Row> newRows, String xCol, String yCol) {

        StructType schema = new StructType()
                .add("id", DataTypes.LongType)
                .add("geohash_numeric", DataTypes.IntegerType)
                .add("bucket_min", DataTypes.IntegerType);

        Encoder<Row> encoder = Encoders.row(schema);

        Dataset<Row> preparedDF = newRows
                .withColumn("extracted_x", col(xCol))
                .withColumn("extracted_y", col(yCol));

        return preparedDF.mapPartitions(
                (MapPartitionsFunction<Row, Row>) iterator -> {
                    List<Row> out = new ArrayList<>();
                    while (iterator.hasNext()) {
                        Row r = iterator.next();


                        Object xVal = r.getAs("extracted_x");
                        Object yVal = r.getAs("extracted_y");
                        Object idVal = r.getAs("id");

                        if (idVal == null || xVal == null || yVal == null) {
                            continue;
                        }


                        long id = ((Number) idVal).longValue();
                        double x = ((Number) xVal).doubleValue();
                        double y = ((Number) yVal).doubleValue();

                        int geohash = GeometryResult.computeGeohashNumeric(x, y);

                        out.add(RowFactory.create(id, geohash, -1));
                    }
                    return out.iterator();
                },
                encoder
        );
    }


}
