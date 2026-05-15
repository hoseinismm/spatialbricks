package ir.smh.spatialbricks.encoder.udf.converttogeometry;

import ir.smh.spatialbricks.encoder.GeometryReader;
import org.locationtech.jts.geom.Geometry;
import java.io.Serializable;

public class geoJsonGeometricalAdapter implements GeometryReader<Geometry>, Serializable {

    @Override
    public Geometry inputToGeometry(Geometry geoJson) throws Exception {
        return geoJson;
    }
}
