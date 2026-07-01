package ir.smh.spatialbricks.decoder;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class WKBParquetDecoder {

    public static Geometry geometryToJTS(Row row) throws ParseException {

        if (row == null) {
            return null;
        }

        byte[] wkb = (byte[]) row.get(0);

        return wkbToJTS(wkb);
    }

    public static Geometry wkbToJTS(byte[] wkb) throws ParseException {
        return new WKBReader().read(wkb);
    }

}

