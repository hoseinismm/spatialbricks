package ir.smh.spatialbricks;

import ir.smh.spatialbricks.createsql.IcebergTableCreator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.expr;

public class BronzeWriter {

    private final SparkSession spark;

    public BronzeWriter(SparkSession spark) {
        this.spark = spark;
    }

    void writeBronze(TableSpec bronze, Dataset<Row> df) throws NoSuchTableException {

        Dataset<Row> output = df;

        if (Arrays.asList(df.columns()).contains("geometry")) {
            output = df.withColumn("geometry", expr("ST_AsGeoJSON(geometry)"));
        }

        createOrAppend(
                output,
                bronze.database(),
                bronze.table()
        );
    }

    private void createOrAppend(
            Dataset<Row> df,
            String database,
            String table
    ) throws NoSuchTableException {

        String fullName = database + "." + table;

        boolean exists = spark.catalog().tableExists(fullName);

        if (!exists) {

            IcebergTableCreator.createIcebergTableFromSchema(
                    spark,
                    df.schema(),
                    database,
                    table
            );

            df.writeTo(fullName).append();
            return;
        }

        // read existing schema
        StructType tableSchema = spark.table(fullName).schema();

        if (!tableSchema.equals(df.schema())) {
            throw new RuntimeException(
                    "Schema mismatch between incoming data and table: " + fullName
            );
        }

        df.writeTo(fullName).append();
    }

}
