package ir.smh.spatialbricks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class BucketService {

    private final SparkSession spark;

    public BucketService(SparkSession spark) {
        this.spark = spark;
    }

    void updateBucket(TableSpec silver) {

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

        BucketManager.Bucket bucket = BucketManager.loadBucket(bucketFileName);

        System.out.println("partition metadata"+partitionStats);


        BucketManager.updateTreeFromStats(partitionStats, bucket);

        System.out.println("Bucket updated to " + bucketFileName);
    }
}
