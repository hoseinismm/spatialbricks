package ir.smh.spatialbricks;

import ir.smh.spatialbricks.encoder.GeometryReader;
import ir.smh.spatialbricks.encoder.udf.CoordinateToGeohashNumericUdfRegistry;
import ir.smh.spatialbricks.encoder.udf.UDFRegistry;

import org.apache.spark.sql.SparkSession;

public final class UdfRegistrar {

    private UdfRegistrar() {}

    static void register(
            SparkSession spark,
            GeometryReader<?> adapter) {

        UDFRegistry.registerAll(
                spark,
                adapter);

        CoordinateToGeohashNumericUdfRegistry
                .registerAll(spark);
    }
}
