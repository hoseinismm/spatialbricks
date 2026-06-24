package ir.smh.spatialbricks.core;

import ir.smh.spatialbricks.createsql.IcebergTableCreator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.spark.sql.functions.expr;

public class BronzeWriter {

    private final SparkSession spark;

    public BronzeWriter(SparkSession spark) {
        this.spark = spark;
    }

    void writeBronze(TableSpec bronze, Dataset<Row> df) throws NoSuchTableException, IOException {

        Dataset<Row> output = df;


        if (Arrays.asList(df.columns()).contains("geometry")) {
            output = df.withColumn("geometry", expr("to_json(geometry)"));
        }

        createOrAppend(
                output,
                bronze.database(),
                bronze.table()
        );
    }

    void writeBronzeBinary(TableSpec bronze, Dataset<Row> df) throws NoSuchTableException, IOException {


        if (Arrays.asList(df.columns()).contains("geometry")) {
            df = df.withColumn(
                    "geometry",
                    expr(
                            "ST_AsBinary(ST_GeomFromGeoJSON(to_json(geometry)))"
                    )
            );
        }

        createOrAppend(
                df,
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
