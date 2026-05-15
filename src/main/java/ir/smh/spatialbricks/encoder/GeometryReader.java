package ir.smh.spatialbricks.encoder;

import org.locationtech.jts.geom.Geometry;

public interface GeometryReader<T> {
    Geometry inputToGeometry(T input) throws Exception;
}

