package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.udf.SparkGeohashUdfs;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class AddOrUpdateGeohashIndex {

    private final SparkSession spark;
    private final BucketServiceForGeohashIndexing bucketService;

    public AddOrUpdateGeohashIndex(SparkSession spark) {
        this.spark = spark;
        this.bucketService = new BucketServiceForGeohashIndexing(spark);
    }

    public void addIndexToUnindexedRows(
            TableSpec silver,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize
    ) throws NoSuchTableException, IOException {

        String fullName = getFullName(silver);

        if (!tableExists(fullName)) {
            return;
        }

        Dataset<Row> table = loadTable(fullName);

        Dataset<Row> unindexedRows = table
                .filter(col("geometry").isNotNull())
                .filter(col("geometry.geohash_partitioning.floor").isNull());

        long unindexedCount = unindexedRows.count();

        System.out.println("unindexedRows = " + unindexedCount);

        if (unindexedCount == 0) {
            return;
        }

        long totalRowsHint = bucketService.updateBucket(silver);

        updateIndex(
                fullName,
                unindexedRows,
                getBucketFileName(silver),
                rowsCapableOfProcessingByDriver,
                maxPartitionSize,
                totalRowsHint,
                true
        );

        System.out.println("Indexing added to unindexed rows.");
    }

    public void updateIndexing(
            TableSpec silver,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize
    ) throws NoSuchTableException, IOException {

        String fullName = getFullName(silver);

        if (!tableExists(fullName)) {
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
                bucketFileName,
                rowsCapableOfProcessingByDriver,
                maxPartitionSize,
                totalRowsHint,
                false
        );

        System.out.println("Reindexing completed.");
    }

    private void updateIndex(
            String fullName,
            Dataset<Row> rows,
            String bucketFileName,
            long rowsCapableOfProcessingByDriver,
            long maxPartitionSize,
            long totalRowsHint,
            boolean onlyUnindexed
    ) throws IOException {

        JavaSparkContext jsc =
                JavaSparkContext.fromSparkContext(spark.sparkContext());

        List<Integer> borders =
                BucketManagerForGeohashIndexing.computeBucketBorders(
                        rows,
                        bucketFileName,
                        rowsCapableOfProcessingByDriver,
                        maxPartitionSize,
                        totalRowsHint
                );

        int[] bucketBorders = borders.stream()
                .mapToInt(Integer::intValue)
                .toArray();

        Broadcast<int[]> broadcastBorders =
                jsc.broadcast(bucketBorders);

        SparkGeohashUdfs.registerFindFloorAndCeilingUdf(
                spark,
                broadcastBorders
        );

        String whereClause = onlyUnindexed
                ? """
                  geometry.geohash_partitioning.floor IS NULL
                  AND geometry IS NOT NULL
                  """
                : "geometry IS NOT NULL";

        spark.sql(String.format("""
                UPDATE %s
                SET geometry.geohash_partitioning =
                    findFloorAndCeiling(
                        geometry.parts[0].coordinates[0].x,
                        geometry.parts[0].coordinates[0].y
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
            return false;
        }
        return true;
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
