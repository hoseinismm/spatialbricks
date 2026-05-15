package ir.smh.spatialbricks.encoder.udf.converttogeometry;

import ir.smh.spatialbricks.encoder.GeometryReader;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.WKTReader;

import java.io.Serializable;

public class WKTReaderAdapter implements GeometryReader<String>, Serializable {
    private static final GeometryFactory geometryFactory = new GeometryFactory(
            new PrecisionModel(PrecisionModel.FLOATING_SINGLE), 4326
    );
    public Geometry inputToGeometry(String wkt) throws Exception {
        WKTReader reader = new WKTReader(geometryFactory);
        return reader.read(wkt);
    }

}
