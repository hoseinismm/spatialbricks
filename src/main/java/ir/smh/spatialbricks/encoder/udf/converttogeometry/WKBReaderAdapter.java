package ir.smh.spatialbricks.encoder.udf.converttogeometry;

import ir.smh.spatialbricks.encoder.GeometryReader;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

import java.io.Serializable;

public class WKBReaderAdapter implements GeometryReader<byte[]>, Serializable {

    @Override
    public Geometry inputToGeometry(byte[] input) throws Exception {
        WKBReader reader = new WKBReader();
        return reader.read(input);
    }
}
