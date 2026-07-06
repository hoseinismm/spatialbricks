package ir.smh.spatialbricks.core;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class BucketService {

    private final SparkSession spark;

    public BucketService(SparkSession spark) {
        this.spark = spark;
    }

    public void updateBucket(TableSpec silver) throws NoSuchTableException, ParseException {

        String fullName = silver.database() + "." + silver.table();
        String metadataTable = fullName + ".partitions";
        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        String bucketPath = Paths.get(silver.path(), bucketFileName).toString();

        boolean exists = spark.catalog().tableExists(fullName);
        if (exists) {

            File f = new File(bucketPath);
            if (!f.exists()) {
                System.out.println("Bucket " + bucketFileName + " does not exist to update");
                return;
            }

            BucketManager.Bucket bucket = BucketManager.loadBucket(bucketPath);

            Table table = Spark3Util.loadIcebergTable(
                    spark,
                    fullName);

            Snapshot snapshot = table.currentSnapshot();

            if (bucket == null) {
                throw new IllegalStateException(
                        "Table " + table.name() + " current table has no bucket to add indexed data. please update table first");
            }
            if (snapshot == null) {
                System.out.println(
                        "Table has no current snapshot. Bucket cannot be updated."
                );
                return;
            }
            long id = snapshot.snapshotId();

            if (bucket.snapshot == id) {


//            Dataset<Row> statstest = spark.read()
//                    .table(metadataTable);
//            statstest.show();

                Dataset<Row> stats = spark.read()
                        .table(metadataTable)
                        .filter(col("partition").isNotNull())
                        .select(
                                col("partition").getField("geometry.bbox_partitioning.region_code").as("region_code"),
                                col("record_count")
                        )
                        .filter(col("region_code").isNotNull())
                        .groupBy("region_code")
                        .agg(sum(col("record_count")).as("total_count"));

                List<Row> partitionStats = stats.collectAsList();

//        System.out.println("partition metadata"+partitionStats);

                BucketManager.updateTreeFromStats(partitionStats, bucket);

                BucketManager.saveBucket(bucket, bucketPath);

                System.out.println("Bucket updated to " + bucketFileName);
                return;
            } else {
                System.out.println("""
            Bucket snapshot differs from the current table snapshot.
            Update table if you think there is a problem.
            """);
            }
        }
        System.out.println("""
            The table does not exist. If a bucket file is present,
            it will be ignored.
            """);
    }
}
