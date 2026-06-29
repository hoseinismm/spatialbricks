package ir.smh.spatialbricks.udf;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;

public class ParseGeometryForWKB {
    private static final WKBWriter writer = new WKBWriter();

    public static byte[] parseGeometry(Geometry geometry) {

        if (geometry == null) {
            return null;
        }

        return writer.write(geometry);
    }

}