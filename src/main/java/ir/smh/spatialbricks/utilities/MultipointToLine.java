package ir.smh.spatialbricks.utilities;


import ir.smh.spatialbricks.udf.UDFRegistry;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT$;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

import static ir.smh.spatialbricks.encoder.GeometryResult.lineFromMultiPoint;

public class MultipointToLine implements Serializable {
    SparkSession spark;
    UDFRegistry<?,?> udfRegistry;
    public MultipointToLine(SparkSession spark, UDFRegistry<?, ?> udfRegistry) {
        this.spark = spark;
        this.udfRegistry = udfRegistry;
    }
    public void registerLineFromMultiPoint() {
        spark.udf().register(
                "multiPointToLine",
                (Row geoRow) -> {
                    Geometry geometry = udfRegistry.geometryToJTS(geoRow);
                    return lineFromMultiPoint(geometry);
                },
                GeometryUDT$.MODULE$
        );
    }
}
