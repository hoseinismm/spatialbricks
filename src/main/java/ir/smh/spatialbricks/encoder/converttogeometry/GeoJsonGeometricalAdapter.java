package ir.smh.spatialbricks.encoder.converttogeometry;

import org.locationtech.jts.geom.*;

import java.io.Serializable;

public class GeoJsonGeometricalAdapter implements GeometryReader<Geometry>, Serializable {

    @Override
    public Geometry inputToGeometry(Geometry geometry) {
     return geometry;
    }
}
