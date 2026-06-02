package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.udf.SparkUdfs;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SpatialIndexBackfillJob {

    private final SparkSession spark;
    private final BucketService bucketService;

    public SpatialIndexBackfillJob(SparkSession spark) {
        this.spark = spark;
        this.bucketService = new BucketService(spark);
    }

    public void execute(
            TableSpec silver,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize

    ) throws NoSuchTableException, IOException {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        String fullName =
                silver.database() + "." + silver.table();

        if (!spark.catalog().tableExists(fullName)) {
            System.out.println(
                    "table " + fullName + " does not exist");
            return;
        }
        String bucketFileName =
                "bucket_"
                        + silver.database()
                        + "_"
                        + silver.table()
                        + ".gz";

        Dataset<Row> table = spark.read()
                .format("iceberg")
                .load(fullName);

        long totalRows = table.count();

        table.show();

        table.printSchema();

        Dataset<Row> unindexed = table
                .filter(col("geometry").isNotNull())
                .filter(col("geometry.partition_number.floor").isNull());

        long unindexedRows = unindexed.count();

        System.out.println("totalRows = " + totalRows);
        System.out.println("unindexedRows = " + unindexedRows);

        if (unindexedRows == 0) {
            return;
        }

        Long totalRowsHint= bucketService.updateBucket(silver);

        List<Integer> borders =
                BucketManager.computeBucketBorders(
                    unindexed,
                    bucketFileName,
                    rowsCapableOfProcessingByDriver,
                    maxPartitionSize,
                    totalRowsHint
                );

        int[] bucketBorders = borders.stream()
                .mapToInt(i -> i)
                .toArray();

        Broadcast<int[]> broadcastBorders =
                jsc.broadcast(bucketBorders);

        SparkUdfs.registerFindFloorAndCeilingUdf(
                spark,
                broadcastBorders
        );

        explainPartitionUpdate(fullName);

        runPartitionUpdate(fullName);

        validate(fullName, totalRows);

        System.out.println("Press Enter to exit...");

    }


    private void explainPartitionUpdate(String fullName) {

        spark.sql(String.format("""
            EXPLAIN EXTENDED
            UPDATE %s
            SET geometry.partition_number =
                findFloorAndCeiling(
                    geometry.parts[0].coordinates[0].x,
                    geometry.parts[0].coordinates[0].y
                )
            WHERE geometry.partition_number.floor IS NULL
              AND geometry IS NOT NULL
            """, fullName))
                .show(false);
    }

    private void runPartitionUpdate(String fullName) {

        spark.sql(String.format("""
            UPDATE %s
            SET geometry.partition_number =
                findFloorAndCeiling(
                    geometry.parts[0].coordinates[0].x,
                    geometry.parts[0].coordinates[0].y
                )
            WHERE geometry.partition_number.floor IS NULL
              AND geometry IS NOT NULL
            """, fullName));
    }

    private void validate(
            String fullName,
            long expectedCount) {

        Dataset<Row> table = spark.read()
                .format("iceberg")
                .load(fullName);

        long actualCount = table.count();

        if (actualCount != expectedCount) {
            throw new IllegalStateException(
                    "Row count mismatch. expected="
                            + expectedCount
                            + ", actual="
                            + actualCount
            );
        }
    }

}
