package ir.smh.spatialbricks.encoder.converttogeometry;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class geoJsonGeometricalAdapter2 implements GeometryReader<Geometry>, Serializable {

    @Override
    public Geometry inputToGeometry(Geometry geometry) {
     return geometry;
    }
}
