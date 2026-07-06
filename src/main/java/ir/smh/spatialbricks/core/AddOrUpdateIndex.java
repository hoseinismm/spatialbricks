package ir.smh.spatialbricks.core;


import ir.smh.spatialbricks.udf.UDFRegistry;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.io.File;
import java.io.IOException;

import static org.apache.spark.sql.functions.col;

public class AddOrUpdateIndex {

    private final SparkSession spark;
    private final BucketService bucketService;
    private final  UDFRegistry<?,?> udfRegistry;

    public AddOrUpdateIndex(SparkSession spark, UDFRegistry<?,?> udfRegistry) {
        this.spark = spark;
        this.bucketService = new BucketService(spark);
        this.udfRegistry = udfRegistry;
    }

    public void addIndexToUnindexedRows(
            TableSpec silver,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize
    ) throws NoSuchTableException, IOException, ParseException {

        String fullName = getFullName(silver);

        if (tableExists(fullName)) {
            return;
        }

        Dataset<Row> table = loadTable(fullName);

        Dataset<Row> unindexedRows = table
                .filter(col("geometry").isNotNull())
                .filter(col("geometry.bbox_partitioning").isNull());

        long unindexedCount = unindexedRows.count();

        System.out.println("unindexedRows = " + unindexedCount);

        if (unindexedCount == 0) {
            return;
        }

        bucketService.updateBucket(silver);

        updateIndex(
                fullName,
                unindexedRows,
                silver,
                rowsCapableOfProcessingByDriver,
                maxPartitionSize,
                true
        );

        System.out.println("Indexing added to unindexed rows.");
    }

    public void updateIndexing(
            TableSpec silver,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize
    ) throws NoSuchTableException, IOException, ParseException {

        String fullName = getFullName(silver);

        if (tableExists(fullName)) {
            return;
        }

        String bucketFileName = getBucketFileName(silver);
        deleteBucketFile(bucketFileName);

        Dataset<Row> rowsToReindex = loadTable(fullName)
                .filter(col("geometry").isNotNull());

        long totalRowsHint = rowsToReindex.count();

        System.out.println("totalRowsToIndex = " + totalRowsHint);

        if (totalRowsHint == 0) {
            System.out.println("No rows have geometry for indexing.");
            return;
        }

        updateIndex(
                fullName,
                rowsToReindex,
                silver,
                rowsCapableOfProcessingByDriver,
                maxPartitionSize,
                false

        );

        System.out.println("Reindexing completed.");
    }

    private void updateIndex(
            String fullName,
            Dataset<Row> rows,
            TableSpec silver,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize,
            boolean onlyUnindexed

    ) throws IOException, NoSuchTableException, ParseException {

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(spark.sparkContext());

        BucketManager.Bucket bucket =
                BucketManager.computeBucketBorders(
                        spark,
                        rows,
                        silver,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        udfRegistry
                );

        Broadcast<BucketManager.Bucket> broadcastRootBuckets =
                jsc.broadcast(bucket);

        udfRegistry.registerBucketUdf(
                broadcastRootBuckets
        );

        String whereClause = onlyUnindexed
                ? """
                  geometry.bbox_partitioning.region_code IS NULL
                  AND geometry IS NOT NULL
                  """
                : "geometry IS NOT NULL";

        spark.sql(String.format("""
                UPDATE %s
                SET geometry.bbox_partitioning =
                    findBucket(
                        geometry
                    )
                WHERE %s
                """,
                fullName,
                whereClause
        ));
    }

    private Dataset<Row> loadTable(String fullName) {
        return spark.read()
                .format("iceberg")
                .load(fullName);
    }

    private boolean tableExists(String fullName) {
        if (!spark.catalog().tableExists(fullName)) {
            System.out.println("table " + fullName + " does not exist");
            return true;
        }
        return false;
    }

    private String getFullName(TableSpec silver) {
        return silver.database() + "." + silver.table();
    }

    private String getBucketFileName(TableSpec silver) {
        return "bucket_"
                + silver.database()
                + "_"
                + silver.table()
                + ".gz";
    }

    private void deleteBucketFile(String bucketFileName) {
        File file = new File(bucketFileName);

        if (file.exists() && !file.delete()) {
            throw new RuntimeException(
                    "There is no previous root bucket: " + bucketFileName
            );
        }
    }
}
