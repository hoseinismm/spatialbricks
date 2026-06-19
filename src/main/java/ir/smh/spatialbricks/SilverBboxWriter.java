package ir.smh.spatialbricks;

import ir.smh.spatialbricks.createsql.IcebergTableCreatorWithPartitioning;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class SilverBboxWriter {

    private final SparkSession spark;

    public SilverBboxWriter(SparkSession spark) {
        this.spark = spark;
    }

    void writeSilver(TableSpec silver, Dataset<Row> transformed) throws NoSuchTableException {

        String fullName = silver.database() + "." + silver.table();
        boolean exists = spark.catalog().tableExists(fullName);

        transformed.printSchema();

        if (!exists) {
            System.out.println("Now creating silver table with ID column...");
            transformed.printSchema();
            IcebergTableCreatorWithPartitioning.createIcebergTableFromSchema(
                    spark,
                    transformed.schema(),
                    silver.database(),
                    silver.table(),
                    List.of("identity(geometry.bbox_partitioning.region_code)")
            );
        } else {

            StructType tableSchema = spark.table(fullName).schema();

            if (!tableSchema.sameType(transformed.schema())) {
                throw new RuntimeException(
                        "Schema mismatch for table: " + fullName
                );
            }
        }

        transformed.writeTo(fullName).append();
        System.out.println("Data appended to silver layer");

        Dataset<Row> table = spark.read()
                .format("iceberg")
                .load(fullName);
        long n1 = table.count();
        System.out.println("Row count n1 = " + n1);
    }
}
