package ir.smh.spatialbricks.udf;

import org.apache.sedona.common.Functions;
import org.locationtech.jts.geom.Geometry;

public class ParseGeometryForWKB {


    public static byte[] parseGeometry(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        return Functions.asWKB(geometry);
    }
}