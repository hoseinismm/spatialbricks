package ir.smh.spatialbricks.encoder.udf;

import ir.smh.spatialbricks.decoder.GeometryDecoder;

import ir.smh.spatialbricks.decoder.GeometryDecoder2;
import ir.smh.spatialbricks.decoder.GeometryDecoder3;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT$;

public class ConvertToSedonaUdfRegistry {

    public static void registerAll(SparkSession spark) {

        spark.udf().register(
                "decodeGeometry",
                (Row geoRow) -> GeometryDecoder3.geometryToJTS(geoRow),
                GeometryUDT$.MODULE$
        );
    }
}

