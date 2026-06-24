package ir.smh.spatialbricks.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class BucketServiceForBboxIndexing {

    private final SparkSession spark;

    public BucketServiceForBboxIndexing(SparkSession spark) {
        this.spark = spark;
    }

    public Long updateBucket(TableSpec silver) {

        String fullName = silver.database() + "." + silver.table();
        String metadataTable = fullName + ".partitions";
        String bucketFileName = "bucket_" + silver.database() + "_" + silver.table() + ".gz";

        File f = new File(bucketFileName);
        if (!f.exists()) {
            System.out.println("Bucket " + bucketFileName + " does not exist to update");
            return null;
        }

        Dataset<Row> statstest = spark.read()
                .table(metadataTable);
        statstest.show();

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

        long totalRowsHint = 0L;

        for (Row row : partitionStats) {
            Long count = row.getAs("total_count");
            if (count != null) {
                totalRowsHint += count;
            }
        }

        BucketManagerForBboxIndexing.Bucket bucket = BucketManagerForBboxIndexing.loadBucket(bucketFileName);

        System.out.println("partition metadata"+partitionStats);

        BucketManagerForBboxIndexing.updateTreeFromStats(partitionStats, bucket);

        System.out.println("Bucket updated to " + bucketFileName);
    return totalRowsHint;
    }
}
